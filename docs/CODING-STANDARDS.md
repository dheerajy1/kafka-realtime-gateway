# Rust Coding Standards

Living document. A new rule is added each time we find a bad practice during review. Each rule has: the rule, why, bad example, good example, and where it was found.

This file is the same in every Rust project of ours (recordlog-api, jobs-api-gateway-rust, kafka-api-gateway-rust). A rule keeps its number in every project. A rule that exists in only one project has a sub-number, for example S7.1, and a project that does not need a rule marks it as skipped with the reason and a "Revisit when" line.

## How to read a rule

- **Rule**: one sentence.
- **Why**: the reasoning, so it can be applied to new cases.
- **Found in**: file and line where we met it.

## S1. Borrow first, own only when you must

**Rule**: Take `&str`, `&[T]` or a generic `AsRef`/`IntoIterator` in function signatures. Use `String` or `Vec` only when the function must keep the data after the caller's value is gone.

**Why**: Every `String` or `Vec<String>` is a heap allocation. If the data is only read once (for example passed to `Command::args`), the allocation is pure waste. Ownership should follow a real need, not silence the compiler.

**Bad**

```rust
fn package_args(command: &str, package: &str, release: bool) -> Vec<String> {
    let mut args = vec![command.to_string(), "--package".to_string(), package.to_string()];
    if release { args.push("--release".to_string()); }
    args
}
```

**Good**

```rust
fn package_args<'a>(command: &'a str, package: &'a str, release: bool) -> impl Iterator<Item = &'a str> {
    [command, "--package", package].into_iter().chain(release.then_some("--release"))
}
```

**Found in**: `xtask/src/main.rs` (`package_args`, `run_step`, `run_verify`).

## S2. Never allocate inside a loop if one buffer can be reused

**Rule**: Create a `Vec` or `String` once before the loop, call `.clear()` at the start of each round, and let the read or write functions fill it. `clear()` empties the contents but keeps the memory.

**Why**: An allocation inside a loop is paid on every round. A reused buffer is paid once, so later rounds cost nothing and memory stays flat.

**Bad**

```rust
for line in reader.split(b'\n') {
    let line = String::from_utf8_lossy(&line?).into_owned();
}
```

**Good**

```rust
let mut raw = Vec::new();
loop {
    raw.clear();
    if reader.read_until(b'\n', &mut raw)? == 0 { break; }
    let line = String::from_utf8_lossy(&raw);
}
```

**Found in**: `xtask/src/quiet.rs` (output reading loop). Applied and checked with `cargo check -p xtask`.

## S3. A tool that spawns cargo must clean the environment it inherited

**Rule**: When a program started by `cargo run` (such as an xtask) spawns another `cargo` command, remove the variables that `cargo run` set for it: `CARGO_PKG_*`, `CARGO_MANIFEST_*`, `CARGO_CRATE_NAME`, `CARGO_BIN_NAME`, `CARGO_PRIMARY_PACKAGE`.

**Why**: Build scripts such as the one in `ring` track these variables. If a nested cargo sees different values than a direct cargo, the two keep invalidating each other's cache and every dependency rebuilds.

**How to check**: `CARGO_LOG=cargo::core::compiler::fingerprint=info cargo verify 2>&1 | grep EnvVarChanged`. It names the variable and shows the old and new values.

**Good**

```rust
for (key, _) in std::env::vars_os() {
    if key.as_encoded_bytes().starts_with(b"CARGO_PKG_") {
        sh.env_remove(&key);
    }
}
```

**Found in**: `xtask/src/quiet.rs`. Warm `cargo verify` went from about 65s to about 1s.

## S4. When you build a new Command from another, copy every setting you need

**Rule**: `Command::get_program()` and `get_args()` return only the program and arguments. Environment changes, working directory and other settings on the old `Command` are not carried over to a new one.

**Why**: `cmd.env_remove(...)` in `run_step` would have been silently ignored, because `quiet::run` created a fresh `Command::new("sh")`. Code that looks correct does nothing.

**Good**: apply the setting on the `Command` that is actually spawned, or pass the original `Command` through without rebuilding it.

**Found in**: `xtask/src/quiet.rs` (`run`) and `xtask/src/main.rs` (`run_step`). The bug was met in recordlog-api first, and kafka-api-gateway-rust had the same `sh -c` pattern.

## S5. Tooling crates must not depend on the application crate

**Rule**: A helper crate such as `xtask` depends only on what it really uses. If it needs something that lives in the application crate (for example the env schema), move that thing into a small shared library crate and make both crates depend on it.

**Why**: A dependency on the application crate brings in its whole dependency tree. Every build of the tool compiles all of it, and the tool rebuilds whenever any application file changes. A small shared crate keeps one source of truth without that cost.

**How to check**: `cargo tree -p <crate> --prefix none | sort -u | wc -l` counts the unique packages a crate pulls in.

**Bad**

```toml
# xtask/Cargo.toml: the whole server tree
[dependencies]
kafka-api-gateway = { path = ".." }
```

**Good**

```toml
# xtask/Cargo.toml: only the env crate and the tools xtask uses
[dependencies]
kafka-env = { workspace = true }
```

**Related**: an `impl` block must be in the crate that defines the type, so shared logic such as `load()` moves into the shared crate together with the type. In `lib.rs`, keep only the module list and re-exports, and put the code in named files.

**Found in**: `xtask/Cargo.toml`. New crate: `crates/kafka-env` (I7, D57 in `docs/build.md`).

## S6. Use the standard library to merge or capture output, not a shell

**Rule**: To read a child process's stdout and stderr as one stream, create `std::io::pipe()`, give one clone of the writer to `.stdout(...)` and the other to `.stderr(...)`, and read from the reader. Do not spawn `sh -c '... 2>&1'` for this. After `spawn()`, `drop` the `Command` so its copy of the writer is closed.

**Why**: A shell adds a process, depends on `sh` being present, and brings quoting rules. The pipe needs none of that and keeps using the original `Command` with all its settings (S4). If the writer is not dropped, the reader never sees the end of the output and the program hangs.

**Bad**

```rust
let mut sh = Command::new("sh");
sh.args(["-c", "exec \"$0\" \"$@\" 2>&1"])
    .arg(command.get_program())
    .args(command.get_args());
```

**Good**

```rust
let (reader, writer) = io::pipe()?;
command.stdout(writer.try_clone()?).stderr(writer);
let mut child = command.spawn()?;
drop(command);
```

**Needs**: Rust 1.87 or newer.

**Found in**: `xtask/src/quiet.rs` (`run`).

## S7. Panic for bugs, print and exit for mistakes the user can make

**Rule**: Use `panic!`, `.expect(...)` and `.unwrap()` for situations that mean the program itself is wrong. When the cause is outside the program (a missing tool, a missing file, a bad argument), print one clear line with the reason and exit with code 1, or return an error.

**Why**: A panic prints a crash report with a source location and exits with code 101, which tells the user the tool is broken. A clear message tells them what to fix. Printing the underlying error (`{error}`) keeps the real reason visible.

**Bad**

```rust
let status = Command::new("docker").status().expect("Failed to execute docker");
```

**Good**

```rust
let status = command.status().unwrap_or_else(|error| {
    eprintln!("Failed to execute docker: {error}");
    exit(1);
});
```

**Found in**: `xtask/src/main.rs` (`status_or_exit`, `load_env`).

## S7.1. Tasks that need application code live in a tools crate, not in xtask

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the rule exists in jobs-api-gateway-rust, where `gen-token` and `extract-mails` need server code (the Google OAuth database access and the extract service). Every `xtask` task here (`css`, `dev`, `start`, `build`, `hash`, `test-ws`, `verify` and the docker tasks) only starts another command, reads the env through `kafka-env`, hashes with `bcrypt` or talks to the running gateway with `tungstenite`. None of them imports the gateway crate, so there is no tools crate to split out.

**Revisit when**: a new `xtask` task needs the database, the Kafka client or any other code of the gateway crate. Put that task in `crates/kafka-tools` and let `xtask` run it, so `cargo tree -p xtask --prefix none | sort -u | wc -l` stays small.

## S8. Put shared code in the smallest crate that owns it, and never keep two copies

**Rule**: When two crates need the same code, move it into a small library crate named for what it holds, and make both depend on it. If a piece brings heavy dependencies (a database driver), give it its own crate, so a crate that needs only the light part does not pay for the heavy one. Delete the old copy in the same change.

**Why**: Cargo compiles every dependency of every crate you depend on, even if you call one function of it. A global written twice (a `LazyLock` static) is two separate values: two loads, two validations, two pools. One copy in one crate gives one global. The crates of a workspace share one `Cargo.lock` and one `target` directory, so a small shared crate is compiled once for all of them.

**How to check**: `cargo tree -p <crate> --prefix none | sort -u | wc -l` before and after the change.

**Bad**

```toml
# a second binary that needs only the env schema
[dependencies]
kafka-api-gateway = { path = ".." }
```

**Good**

```toml
# the same binary with the small shared crate
[dependencies]
kafka-env = { workspace = true }
```

**Related**: Delete a file only after every user of it has switched, otherwise the build breaks in between. A one-line `pub use` in the old place (`src/config/env.rs`) keeps old import paths working, so the files that use it need no edit.

**Found in**: `src/config/env.rs`, which is now one `pub use kafka_env::{EnvSchema as Env, env, init};` line, so the files that import it did not change. `env()` and `init()` live in `crates/kafka-env`. In recordlog-api the same rule moved the Azure pools into `crates/recordlog-azure`.

## S9. If a function must keep or change the data, take it by value and move it

**Rule**: When a function keeps or edits its input, take the input by value and move the parts out of it. Do not take a reference and then call `clone()`, `cloned()` or `to_string()` to get an owned copy. The caller either hands over data it no longer needs, which is a move with no copy, or clones once on purpose.

**Why**: A reference plus a clone means two full copies exist for a while, and the cost grows with the size of the data. Taking ownership puts the question at the call site: does anyone still need the original? A move copies only the small handle (pointer, length, capacity), not the data behind it. S1 is the other side of the same idea: borrow when you only read, own when you keep or edit.

**Bad**

```rust
async fn verify_password(password: &str, hash: &str) -> Result<bool, ApiError> {
    // Two copies, made only because the blocking thread needs its own texts
    let password = password.to_string();
    let hash = hash.to_string();

    let checked = tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash)).await;
    // ...
}
```

**Good**

```rust
// The caller hands over the texts it no longer needs, so they move into the thread
async fn verify_password(password: String, hash: Option<String>) -> Result<bool, ApiError> {
    let checked = tokio::task::spawn_blocking(move || match hash {
        Some(hash) => bcrypt::verify(password, &hash),
        None => bcrypt::verify(password, DUMMY_HASH),
    })
    .await;
    // ...
}
```

**Related**: A move must come after the last use of anything that borrows from the moved value. In `run_test_ws` the `format!` that reads `env.app_port` comes before the lines that move `env.client_id` and `env.client_secret`. Moving one field out of a struct that has no `Drop` is allowed, and the other fields stay usable.

**Found in**: `src/auth/password.rs` (`verify_password`), `src/features/v1/auth/service.rs` (`login`) and `xtask/src/main.rs` (`run_test_ws`).

## S10. Before cloning to call an owning function, look for its borrowed form

**Rule**: When a function takes its argument by value and you only hold a reference, do not clone to bridge the gap before checking whether the library offers a borrowed form. Many do: `serde_json::to_string(&value)` takes a reference, so a `#[derive(Serialize)]` struct that borrows its fields replaces `json!(...)`, which clones every field it is given. On the read side, `&Value` is a serde `Deserializer`, so `T::deserialize(&value)` replaces `serde_json::from_value(value.clone())`.

**Why**: A clone made only to satisfy a signature copies data that nothing needs. A borrowed form reads the data where it is. The compiler cannot warn about this clone, because it is valid code, so it has to be found by reading.

**Bad**

```rust
let text = json!({ "correlationId": correlation_id.clone(), "jobName": job_name.clone() }).to_string();
```

**Good**

```rust
let text = PublishedEvent { correlation_id: &correlation_id, job_name: &job_name, /* ... */ }.to_text();
```

**Related**: S9 covers the other case, where the function needs ownership and the caller no longer needs the value.

**Found in**: `PublishedEvent` in `src/features/v1/ws_publish/model.rs` (I31), `Frame` in `src/ws/frames.rs` (I65) and `EventFrame` in `src/kafka/inbound.rs` (I18.1). In recordlog-api the read side was `RecordLogEvent::parse` (I20, skipped in kafka-api-gateway-rust).

## S11. A type that holds secrets must not derive Debug or Clone

**Rule**: Do not derive `Debug` on a type that holds a password, a key or a token. Write `Debug` by hand and list only the fields that are safe to show, an allowlist, then end with `finish_non_exhaustive()`. Leave out `Clone` unless a copy is really needed.

**Why**: `Debug` is what `{:?}`, `dbg!` and a failed `unwrap` print, so a derived `Debug` puts every secret into the logs the first time someone prints the value. Every `Clone` leaves one more copy of the secrets in memory. The Rust API Guidelines (C-DEBUG) say all public types implement `Debug` and that exceptions are rare. A hand-written `Debug` keeps the guideline and still hides the secrets. With an allowlist, a field added later stays hidden until someone adds it on purpose.

**Bad**

```rust
#[derive(Clone, Debug, Deserialize)]
pub struct EnvSchema {
    pub jwt_secret: String,
}
```

**Good**

```rust
#[derive(Deserialize)]
pub struct EnvSchema {
    pub app_port: u16,
    pub jwt_secret: String,
}

impl fmt::Debug for EnvSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvSchema")
            .field("app_port", &self.app_port)
            .finish_non_exhaustive()
    }
}
```

**Found in**: `crates/kafka-env/src/schema.rs` (`EnvSchema`, I24), `NoAuthHeaders` in `src/schemas/auth.rs` (I26), `ApiUserRecord` and `ApiKeyRecord` in `src/db/store.rs` (I28) and `LoginRequest` and `LoginForm` (I75).

## S12. When a value is borrowed on one path and owned on another, use `Cow`

**Rule**: if a struct is filled from borrowed text when it is written out and from owned text when it is read in, give its text fields the type `Cow<'a, str>`. `Cow` means "either borrowed or owned". The write path uses `Cow::Borrowed`, so it copies nothing. The read path holds owned text. Call `into_owned` only where a `String` is needed.

**Why**: the std docs describe `Cow` as holding borrowed data and cloning it only when ownership or mutation is needed. `into_owned` on a `Cow::Owned` moves the data out without a clone, and `From<&str>` for `Cow` does not allocate. One struct with `Cow` replaces either a copy on every write or a second, borrowed twin of the struct that must be kept in step.

**Bad**

```rust
pub struct Claims { pub sub: String, pub role: String }

let claims = Claims { sub: username.to_string(), role: role.to_string() };
```

**Good**

```rust
pub struct Claims<'a> { pub sub: Cow<'a, str>, pub role: Cow<'a, str> }

let claims = Claims { sub: Cow::Borrowed(username), role: Cow::Borrowed(role) };
```

**Found in**: `Claims` in `src/auth/jwt.rs` (I27), `ApiError` and `ErrorResponse` in `src/errors/api_error.rs` (I34) and the `label` of `Row` in `src/components/health_parts.rs` (I36), where kafka needs `Cow` because the labels such as `Broker 1` are built at run time.

## S13. A package built on its own must name the features it needs itself

**Rule**: if a binary needs a feature of a dependency to run, list that feature in that package's own `Cargo.toml`. Do not rely on another package in the workspace to turn it on. A feature is a switch inside a crate that turns optional code on.

**Why**: Cargo builds a dependency once, with the union of the features that every package asks for (Cargo Book, Features). `cargo test` builds the whole workspace, so a feature that one package asks for quietly helps all the others. A build of one package alone, such as `cargo build --release --package subscriber` in Docker, gets no such help. `cargo tree -e features -i <crate>` shows which package turns on which feature.

**Bad**

```toml
tokio-tungstenite = { version = "0.28", default-features = false, features = ["connect", "rustls-tls-webpki-roots"] }
```

**Good**

```toml
tokio-tungstenite = { version = "0.28", default-features = false, features = ["connect", "rustls-tls-webpki-roots"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12", "logging"] }
```

**Found in**: not met in kafka-api-gateway-rust yet, because the gateway is the only runnable package. It came from `crates/subscriber/Cargo.toml` in recordlog-api, the `rustls` line that picks the `ring` provider (see I50). The rule stays so the file is the same in every project.

## S14. Read global state at the edge, pass values into functions as arguments

**Rule**: read the env and other global state in one place at the edge of the program (for example in `check_client_credentials`), and pass the values into the functions below it as arguments. A function that takes its inputs as arguments can be tested with values the test chooses, such as the address of a fake server.

**Why**: there is no official source for this rule, I did not find one that I opened. It comes from the chunking work in recordlog-api (I53): the function that published the chunks read the env itself, so no test could reach a successful publish. In kafka-api-gateway-rust the client check and the login take the expected values as arguments, so a test passes its own id, secret and JWT secret and never sets an env variable for them.

**Bad**

```rust
fn check_client_headers(headers: &HeaderMap) -> Result<(), ApiError> {
    let env = env();
    // the expected id and secret come from the env, a test cannot change them
    compare(headers, &env.client_id, &env.client_secret)
}
```

**Good**

```rust
// The edge reads the env once and the function below it takes plain values
fn check_client_headers(headers: &HeaderMap, expected_id: &str, expected_secret: &str) -> Result<(), ApiError> {
    compare(headers, expected_id, expected_secret)
}
```

**Found in**: `check_client_headers` and `check_client_credentials` in `src/middleware/no_auth.rs`, and `login` in `src/features/v1/auth/service.rs`, which takes `jwt_secret` as an argument (I72). In `run_test_ws` in `xtask/src/main.rs` the env is read once and its texts are moved into the arguments.

## S15. Compare secrets with a constant-time function, never with `==`

**Why**: `==` on strings or bytes stops at the first byte that differs, so a wrong first byte is answered a little faster than a wrong last byte. An attacker who measures that can learn a secret one byte at a time. The `subtle` crate exists for this, and several Rust crates that check tokens and signatures use `ConstantTimeEq`.

**Bad**

```rust
if given_secret != env.client_secret {
    return Err(rejected());
}
```

**Good**

```rust
use subtle::ConstantTimeEq;

let secret_ok = given_secret.as_bytes().ct_eq(env.client_secret.as_bytes());
let id_ok = given_id.as_bytes().ct_eq(env.client_id.as_bytes());
// One & compares both, a && would skip the secret when the id is wrong
if !bool::from(id_ok & secret_ok) {
    return Err(rejected());
}
```

**Found in**: `check_client_headers` in `src/middleware/no_auth.rs` (I58). `subtle` is a direct dependency of the gateway (`subtle = "2"`) and was already in `Cargo.lock` through `bcrypt` and `jsonwebtoken`.

## S16. Build fixed output once in a `LazyLock` and lend it as `&'static`

**Why**: a page, a JSON document or a set of keys that is the same for every request should be built on the first request and reused. A handler that returns `Html(&'static str)` does no work and no copy per request. Do not use this for output that changes, such as the health pages.

**Bad**

```rust
async fn about() -> Html<String> {
    render_with_props(About, props())
}
```

**Good**

```rust
static ABOUT_HTML: LazyLock<String> = LazyLock::new(|| render_with_props(About, props()).0);

async fn about() -> Html<&'static str> {
    Html(ABOUT_HTML.as_str())
}
```

**Found in**: `src/pages/home.rs`, `about.rs`, `login.rs`, `src/openapi/mod.rs` (the JSON and the Scalar page) and the header and the validation rules in `src/auth/jwt.rs` (I59, I60, I63, I37). The JWT keys are not cached, because the secret is passed in as an argument (S14), so the keys are built per call.

## S17. Declare a dependency used by more than one crate once, in `[workspace.dependencies]`

**Why**: the Cargo Book lets a workspace declare a dependency once. Crates then write `workspace = true`, so versions and features cannot drift apart. Keep `version` out of `[workspace.package]` while a release tool edits the version in `Cargo.toml`.

**Bad**

```toml
# in two manifests
validator = { version = "0.21", features = ["derive"] }
```

**Good**

```toml
# root Cargo.toml
[workspace.dependencies]
validator = { version = "0.21", features = ["derive"] }

# every crate
validator = { workspace = true }
```

**Found in**: the root `Cargo.toml` (`kafka-env`, `bcrypt`, `serde`, `serde_json`, `validator`) and the manifests of `crates/kafka-env` and `xtask` (I64).

## S18. Read a value that is used once with borrowed fields, not with owned ones

**Rule**: when a struct is read from input that stays alive while the struct is used, give its text fields the type `&'a str`. serde borrows them from the input, and a `&'a Value` input works too. Keep an owned field only where the data has no borrowed form, such as an owned `Value`.

**Why**: serde's lifetimes guide says a `&'a str` field borrows from the input when the format can lend it, and `serde_json::Value` always can, because it already holds the unescaped text. An owned `String` field copies text that is already in memory. The cost is the lifetime parameter: the value cannot outlive its input, and a test must keep the input in a variable.

**Bad**

```rust
struct NoAuthHeaders {
    client_id: String,
    client_secret: String,
}
```

**Good**

```rust
struct NoAuthHeaders<'a> {
    client_id: &'a str,
    client_secret: &'a str,
}

// For serde input, a nested struct that carries the lifetime asks serde to borrow it too
#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    job: Job<'a>,
    #[serde(borrow)]
    records: Vec<&'a str>,
}
```

**Found in**: `NoAuthHeaders<'a>` in `src/schemas/auth.rs` (I26), which borrows the two header values. In recordlog-api the serde case is `RecordLogEvent` in `crates/subscriber/src/event.rs` (I66, skipped in kafka-api-gateway-rust). The JSON readers of kafka (`parse_publish_request`, `parse_client_message`) still return owned `String`s and were not changed in this pass.

## S19. A login does the same work for a missing user as for a wrong password

**Rule**: when a login looks the user up and then checks a slow hash, run the hash check for a missing user too, against a fixed dummy hash, and return false for it. Give the dummy hash the same cost as the real ones.

**Why**: bcrypt is slow on purpose. A login that returns before the check for an unknown user answers much faster than one for a known user, so the response time tells an attacker which user names exist, even when both answers have the same text. The dummy hash must be a valid hash with the same cost, or the check fails fast with an error.

**Bad**

```rust
let Some(hash) = find_hash(&name).await? else {
    return Err(unauthorized());
};

let is_valid = verify_password(password, hash).await?;
```

**Good**

```rust
// None is checked against a dummy hash inside, and never matches
let hash = find_hash(&name).await?;

let is_valid = verify_password(password, hash).await?;
```

**Found in**: `verify_password` in `src/auth/password.rs`, called by `login`, the portal login in `src/pages/login.rs` (I74) and `check_api_key` in `src/middleware/api_key.rs` for a refused API key (I74.1).

## S20. Convert numbers with a method that says what happens out of range

**Rule**: do not use `as` for a conversion that can lose data (a wider to a narrower type, signed to unsigned, a float to an integer). Use `try_from`, `unsigned_abs` or a small helper, and decide in the code what happens when the value does not fit. `as` is fine when the range is proven, with a comment that says why.

**Why**: `as` never fails. A negative number becomes a huge one and a large one is cut, with no error and no warning. `u64::try_from(x).unwrap_or(u64::MAX)` says in the code what the cap is.

**Bad**

```rust
let uptime_ms = started.elapsed().as_millis() as u64;
```

**Good**

```rust
// Whole milliseconds of a duration, capped at u64::MAX instead of wrapping around
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
```

**Found in**: `login` in `src/features/v1/auth/service.rs` (`i64::try_from` for the expiry) and `millis` in `src/features/v1/health/service.rs` (I76). The digit cast in `src/components/health_parts.rs` stays, with its reason in a comment.

## S21. Test network code against a fake peer inside the test

**Rule**: code that talks to a socket gets a test that starts the other side itself, on `127.0.0.1:0` (the system picks a free port), and checks what it received and how the code reacts to every answer, including an error and a closed connection. Wait for a condition with a loop and a time limit, never with one long sleep. Use a library the project already depends on for the fake, so `Cargo.lock` does not change.

**Why**: the logic that decides what is retried, acknowledged or dropped sits next to the socket, and a mock of the socket hides the bugs that matter (ordering, reconnects, a missing answer). A fake on a local port runs in milliseconds and needs no real service.

**Bad**

```rust
// Only the helper that builds the url is tested, `run` is never called
#[test]
fn builds_url() { /* ... */ }
```

**Good**

```rust
let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
let address = listener.local_addr().unwrap();

// The fake gateway accepts the connection with `accept_hdr_async`, records the frames
// and answers each publish; the test then points the config at `address`
```

**Found in**: `tests/ws_socket.rs` (I73.1). The real router listens on `127.0.0.1:0` and a real `tokio-tungstenite` client connects, so the handshake, the close codes 4001 and 1001 and the fan-out to two sockets run end to end. In recordlog-api the same rule is `crates/subscriber/tests/gateway_ws.rs` (I73, skipped in kafka-api-gateway-rust).

## S22. A start-up error names the variable, never its value

**Rule**: when the env fails to load, print the name of the variable and what is wrong (missing, invalid, not one of the allowed words). Never print the value that was read. Build the schema from name and value pairs, so a test can pass a fake env instead of changing the real one.

**Why**: the start-up error is the one message that is always read, in logs, in CI output and in screenshots. A parse error that quotes the value prints a secret that was put in the wrong variable.

**Bad**

```rust
envy::from_env::<EnvSchema>().map_err(|error| error.to_string())
```

**Good**

```rust
envy::from_iter::<_, EnvSchema>(pairs)
    .map_err(|error| describe_envy_error(&error.to_string()))
```

**Found in**: `EnvSchema::from_pairs` and `describe_envy_error` in `crates/kafka-env/src/env.rs`, with tests in `crates/kafka-env/tests/env.rs` (I72.1, I78). Every validator rule in `crates/kafka-env/src/schema.rs` has its own `message`, which names the variable and holds no value.

## Sources

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/): S1 follows C-CALLER-CONTROL and C-GENERIC.
- [The Rust Performance Book, Heap Allocations](https://nnethercote.github.io/perf-book/heap-allocations.html): heap allocations are moderately expensive; profile before removing clones.
- [Cargo Book, Workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html): S8. Members share one Cargo.lock and one output directory.
- [Rust std, `Cow`](https://doc.rust-lang.org/std/borrow/enum.Cow.html): S12. Borrowed data is cloned only when ownership or mutation is needed.
- [Cargo Book, Features](https://doc.rust-lang.org/beta/cargo/reference/features.html): S13. A dependency used by several packages is built with the union of their features, and `cargo tree -e features -i <crate>` shows where each feature comes from.
- [`subtle` crate](https://docs.rs/subtle): S15. Constant-time equality for secrets.
- [Cargo Book, Workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html): S17. `[workspace.dependencies]` and `workspace = true`.
- [serde, Deserializer lifetimes](https://serde.rs/lifetimes.html): S18. Borrowing data in a derived impl.
- [Rust std, `TryFrom`](https://doc.rust-lang.org/std/convert/trait.TryFrom.html): S20. A conversion that can fail returns a `Result`.
- [Rust std, `char_indices`](https://doc.rust-lang.org/std/primitive.str.html#method.char_indices): I71. Byte positions of characters, used to cut a text without splitting one.
- [axum, `Utf8Bytes`](https://docs.rs/axum/0.8.9/axum/extract/ws/struct.Utf8Bytes.html): I22.1. A WebSocket text that is shared with `Clone` instead of copied.
