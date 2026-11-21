# Rust Improvements: reusable checklist

Changes we agreed on in our Rust projects, written so they can be reused on another Rust project. An entry is added only after it is agreed and applied. The same file is kept in every Rust project of ours (recordlog-api, jobs-api-gateway-rust, kafka-api-gateway-rust).

The numbers are the same in every project. An improvement that exists in one project only has a sub-number, for example I15.1 (jobs only) or I18.1 (kafka only). An improvement that does not fit a project is kept in its place and marked "Skipped in <project>", with the reason and a "Revisit when" line, so the numbers never shift.

This copy is for kafka-api-gateway-rust. The `Before` and `After` texts describe this repository. Nothing in the 2026-11-21 pass was compiled where it was written (no network, no cargo registry). It was then checked on the owner's machine: `cargo verify` passed (fmt, check, clippy with `-D warnings`, test), and `cargo dev`, `cargo start`, `cargo build-prod`, `cargo docker-build` and `cargo docker-up` ran correctly. A `Check` line that says `cargo verify` passes is therefore a result that was seen, except where the line says otherwise. The sandbox itself could run `rustfmt --edition 2024`, std-only compiles and a type check of `xtask` against stub crates, but not `cargo check`, `clippy` or `cargo test`.

## I1. Borrow arguments instead of copying them (xtask)

**Before**: `package_args(command, package, release)` returned `Vec<String>`, built with `.to_string()` for every argument. `run_step` took `&[String]`, `build_quiet` was a one-line wrapper around it, and `run_verify` built `Vec<&str>` steps and then turned each into `Vec<String>` with `.map(str::to_string).collect()` before every call.

**After**: `package_args(command, release)` returns `impl Iterator<Item = &str>`. `run_step` takes `I: IntoIterator<Item = S>, S: AsRef<OsStr>`. `run_verify` keeps its steps as `&[&str]` slices in a fixed array and passes them as they are. `build_quiet` is deleted, the callers call `run_step(.., Show::All)`.

**Why**: `Command::args` only reads the text. Copying every argument into a `String` allocates for nothing.

**Check**: `cargo verify` passes.

**Rule**: see S1 in the Rust Coding Standards doc.

## I2. Reuse one buffer when reading lines (xtask quiet.rs)

**Before**: `for line in BufReader::new(stdout).split(b'\n')` created a new `Vec<u8>` for every line, then `String::from_utf8_lossy(&line?).into_owned()` copied it into a new `String`. Two heap allocations per line of cargo output.

**After**: `let mut raw = Vec::new();` is created once before a `loop`. Each round calls `raw.clear()` then `reader.read_until(b'\n', &mut raw)?`, and `String::from_utf8_lossy(&raw)` only borrows when the bytes are valid UTF-8. The same memory is reused for every line.

**Detail**: `read_until` keeps the trailing newline, `split` dropped it, so the code pops it to keep the output identical. A return of `0` means end of input.

**Why**: allocating inside a loop repeats the cost on every round. Clearing a buffer keeps its memory, so later rounds allocate nothing.

**Check**: `cargo verify` passes.

**Rule**: see S2 in the Rust Coding Standards doc.

## I3. Return borrowed pieces instead of a new String (xtask quiet.rs)

**Before**: `fn progress_text(plain: &str) -> Option<String>` built `format!("{first} {name}")`, a new `String` for every progress line, only to print it once.

**After**: `fn progress_text(plain: &str) -> Option<(&str, &str)>` returns the two words as borrowed slices of the input line, and the caller prints them with `write!(out, "\r\x1b[2K   {verb} {name}")`. No allocation.

**Detail**: the returned `&str`s live inside `plain`, so the compiler infers they cannot outlive it. No explicit lifetime is needed because there is only one input reference. The unit tests compare with `Some(("Compiling", "ring"))`.

**Check**: `cargo verify` passes, including the `xtask` tests.

**Rule**: see S1 in the Rust Coding Standards doc.

## I4. Stop `cargo verify` from invalidating the build cache (xtask quiet.rs)

**Problem**: `cargo xtask` is `cargo run -p xtask`. `cargo run` sets variables such as `CARGO_MANIFEST_DIR` and `CARGO_PKG_NAME` for the xtask process, and every nested `cargo` command inherits them. The build script of `ring` tracks these variables, so a nested cargo and a direct cargo see different values and keep invalidating each other. This was found in recordlog-api with `CARGO_LOG=cargo::core::compiler::fingerprint=info cargo verify`, which printed `EnvVarChanged` with the variable name and the old and new values. There, a warm `cargo verify` went from check 8.6s, clippy 2.3s, test 54s to 0.24s, 0.28s and 0.27s.

**Before**: the `sh` command in `quiet::run` of kafka-api-gateway-rust inherited the full environment of the xtask process, the same code as in the other project. The warm-cache numbers were not measured here.

**After**: `quiet::clean_cargo_env(&mut Command)` removes every inherited variable that `cargo run` sets (`CARGO_PKG_*`, `CARGO_MANIFEST_*`, `CARGO_CRATE_NAME`, `CARGO_BIN_NAME`, `CARGO_PRIMARY_PACKAGE`), and `quiet::run` calls it on the command that is spawned.

**Why**: one rule in one place. A variable that is the same for the nested and the direct cargo cannot invalidate anything.

**Check**: `cargo verify` passes. After the first `cargo verify`, a second one on an unchanged tree should take about a second per step. That must be checked by hand.

**Rule**: see S3 and S4 in the Rust Coding Standards doc.

## I5. Reuse one String when stripping colour codes (xtask quiet.rs)

**Before**: `fn strip_ansi(line: &str) -> String` created a new `String` for every line of cargo output and the caller threw it away after one use.

**After**: `fn strip_ansi_into(line: &str, plain: &mut String)` clears the caller's `String` and fills it. `quiet::run` creates `let mut plain = String::new();` once before the loop and passes it in each round.

**Detail**: the `(verb, name)` pair returned by `progress_text` borrows from `plain`. It is used before the next round clears `plain`, so the borrow is valid.

**Check**: `cargo verify` passes.

**Rule**: S2 in the Rust Coding Standards doc (reuse a buffer inside a loop).

## I6. One shared function to clean the cargo environment (xtask main.rs, quiet.rs)

**Before**: `run_server` in `main.rs` spawns `cargo run` directly with `Command::new("cargo")` and did not clean the inherited variables, so the dev server could invalidate the build cache the same way `verify` did. Not measured, it follows from the cause in I4.

**After**: the loop is `pub fn clean_cargo_env(command: &mut Command)` in `quiet.rs`. `quiet::run` and `run_server` both call it.

**Why**: one rule in one place. If the list of variables ever changes, there is one function to edit.

**Check**: `cargo verify` passes.

**Rule**: S3 and S4 in the Rust Coding Standards doc.

## I7. Move the env schema into a small crate so xtask stops depending on the server (crates/kafka-env)

**Problem**: `xtask` depended on the whole `kafka-api-gateway` crate only to read `Env` (the validated env schema and its `app_port`, `client_id` and `client_secret`). That pulled axum, tokio, sqlx, kafkit-client, dioxus and more into every `cargo xtask` build. The package counts before and after were not measured here, because `cargo tree` needs the registry.

**Before**: `xtask -> kafka-api-gateway (whole server)`, the schema in `src/schemas/env.rs`, `load()` in `src/config/env.rs`.

**After**: new library crate `crates/kafka-env` with `src/schema.rs` (`EnvSchema` and `AppEnv`), `src/env.rs` (`EnvSchema::load()`, `from_pairs`, `env()` and `init()`) and `src/lib.rs` (the module list and re-exports). `xtask` and the gateway depend on it, and the root workspace lists it as a member. `src/config/env.rs` is now one line, `pub use kafka_env::{EnvSchema as Env, env, init};`, so no other import changed. `src/schemas/env.rs` is deleted.

**Detail**: an `impl EnvSchema` block must live in the crate that defines the struct, so `load()` moved with the schema. The crate name was chosen by the owner (D57 in `docs/build.md`). `Cargo.lock` was edited by hand for the new package, and the first `cargo check` may rewrite it.

**Check**: `cargo verify` passes. `cargo tree -p xtask --prefix none | sort -u | wc -l` should print far fewer packages than `cargo tree -p kafka-api-gateway --prefix none | sort -u | wc -l`, and that is not measured yet.

**Rule**: see S5 and S8 in the Rust Coding Standards doc.

## I8. Merge cargo output with std::io::pipe instead of an extra sh process (xtask quiet.rs, main.rs)

**Before**: `quiet::run(command: &Command, ...)` started `sh -c 'exec "$0" "$@" 2>&1'` to merge cargo's stdout and stderr, and rebuilt a new `Command` from `get_program()` and `get_args()`.

**After**: `quiet::run(mut command: Command, ...)` creates `let (pipe_reader, pipe_writer) = io::pipe()?;`, gives `pipe_writer.try_clone()?` to `.stdout(...)` and `pipe_writer` to `.stderr(...)` of the original command, sets `CARGO_TERM_COLOR=always`, spawns it, and reads from `pipe_reader`. `run_step` passes `cmd` by value.

**Detail**: the `Command` keeps its own copy of the write end of the pipe, so the read loop would never see the end of the output. `drop(command)` right after `spawn()` closes it. Without that line `cargo verify` hangs. `std::io::pipe` is stable since Rust 1.87.

**Why**: one process less, no dependency on `sh`, no shell quoting, and the original `Command` with its settings is the one that runs (rule S4).

**Check**: `cargo verify` passes and finishes. A hang here would mean the `drop(command)` line is missing.

**Rule**: see S6 in the Rust Coding Standards doc.

## I9. One helper for the repeated exit with a command's exit code (xtask main.rs)

**Before**: `exit(status.code().unwrap_or(1));` was written in `run_css`, in the non-Unix branch of `run_server` and in the docker log task.

**After**: `fn exit_with(status: ExitStatus) -> !` holds the line once with a comment that explains why `1` is used (a signal ended the command, so there is no exit code). The places call `exit_with(status)`.

**Detail**: the return type `!` tells the compiler the function never returns, so code after the call is not needed and the branches of a `match` still agree on their type.

**Why**: a rule written in one place can change in one place.

**Check**: `cargo verify` passes.

## I10. One helper for build then run (xtask main.rs)

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the gateway is the only package `xtask` builds and runs. `run_server` is the one function that builds quietly and then starts the server, and `dev` and `start` both call it. The source projects have a second runner (a subscriber) that made a shared helper worth having.

**Revisit when**: a second package gets its own `cargo dev-...` task. Then move the build, `clean_cargo_env`, `--quiet` and the run into one helper that takes the package name.

## I11. Clear error messages instead of panics for failures the user can cause (xtask main.rs)

**Before**: a missing `tailwindcss` ended in `.expect("Failed to execute tailwindcss, ...")`, a missing env file in `panic!("Failed to load environment file: ...")`, and the non-Unix run in `.expect(...)`. A panic prints a Rust crash report with a source location and exits with code 101, which looks like a bug in the tool.

**After**: `fn status_or_exit(command: &mut Command, failure: &str) -> ExitStatus` runs a command and, if it cannot start, prints `failure: reason` and exits with code 1. `run_css`, `docker`, `compose`, `run_docker_logs_clear` and the non-Unix run use it. `load_env` prints the file name and the reason and exits with code 1.

**Detail**: the underlying error is now printed too (for example "No such file or directory"), which the old fixed messages left out.

**Check**: `cargo verify` passes. These error paths only run when something is missing, so `verify` does not exercise them.

**Rule**: see S7 in the Rust Coding Standards doc.

## I12. Pass the compose file path to the shell as an argument (xtask main.rs)

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: no shell script is built here. `compose` passes `COMPOSE_FILE` to `docker` with `.args(["compose", "-f", COMPOSE_FILE])`, and `run_docker_logs_clear` runs `docker inspect` and `sudo truncate` with the log path as an argument. Nothing is pasted into a shell string.

**Revisit when**: a task builds a shell script with `format!`. Pass every outside value as an argument, then.

## I13. Remove a parameter that never varies (xtask main.rs)

**Before**: `package_args(command, package, release)` and every call passed the same package, `"kafka-api-gateway"`.

**After**: the package is the constant `PACKAGE` in `main.rs` and `package_args(command, release)` has two parameters. The error message of the failed `cargo run` uses the same constant.

**Why**: a parameter that always has the same value suggests flexibility that does not exist, and makes a reader check every caller. If a second package appears later, the parameter can come back.

**Check**: `cargo verify` passes.

## I14. Let the caller name the build task instead of guessing it from the profile (xtask main.rs)

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: there is no `run_built` here. `cargo start` already builds in release mode and then runs (I23), so no message tells the user to run a build task first.

**Revisit when**: a task prints a hint with the name of another task. Pass the name in from the caller then.

## I15. Comment every function and keep the comments true (xtask main.rs, quiet.rs)

**Before**: `package_args`, `run_step`, `build_quiet` and `run_verify` had no comment while every other function did, and the comment above `progress_text` said it returns text such as "Compiling ring".

**After**: every function in `main.rs` and `quiet.rs` has a one-line comment on its own line above it, and the `progress_text` comment says it returns the verb and the crate name, such as `("Compiling", "ring")`.

**Why**: a comment that describes old behaviour is worse than none. When a change alters what a function returns, its comment is part of the change.

**Check**: `cargo verify` passes. The comment edits change no code.

## I15.1. Move tasks that need the server into a tools crate (xtask main.rs, crates/jobs-tools)

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: this entry exists only in jobs-api-gateway-rust, where `gen-token` and `extract-mails` call server code. Every `xtask` task here either starts another command, reads the env through `kafka-env`, hashes with `bcrypt` or talks to the running gateway through `tungstenite`. None imports the gateway crate (see S7.1).

**Revisit when**: a new `xtask` task needs the database, the Kafka client or any other code of the gateway crate. Put it in `crates/kafka-tools`.

## I15.2. Move the logger into its own small crate (crates/jobs-logging)

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: this entry exists only in jobs-api-gateway-rust, where a subscriber and a tools crate share the logger. Here the logger is one file, `src/logging.rs`, and only the gateway uses it. `xtask` prints with `println!`.

**Revisit when**: a second binary or crate needs the logger. Move `init()` and `iso_now_ist()` into `crates/kafka-logging` and leave a one-line `pub use` in `src/logging.rs`.

## I16. Move the subscriber off the API crate

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: there is no subscriber crate. The gateway is the only runnable package, and the WebSocket subscribers of this project are clients that connect from outside.

**Revisit when**: a second package that depends on the gateway crate appears. Move what it uses into small crates first (S8).

## I17. Lend the topics to the gateway instead of cloning them

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the code that cloned topic names at subscriber start-up does not exist here. The topic registry is built once from the database and read through `TopicRegistry::snapshot()`.

**Revisit when**: a start-up path copies a list of names that lives as long as the program.

## I18. Move the event frame instead of copying it

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the entry is about the subscriber binary, which parses a gateway frame. The gateway side of the same idea is the kafka-only I18.1.

**Revisit when**: a binary in this repository parses frames from the gateway.

## I18.1. Move the payload and borrow the headers when the event frame is built (src/kafka/inbound.rs)

**Before**: `event_frame` built a `serde_json::Map` for the fixed fields and a second `Map` for the headers (one clone of every header name and value), moved the payload entries in and ended with `Value::Object(frame).to_string()`. It ran once for every Kafka message that reaches a subscriber.

**After**: `EventFrame<'a>` borrows the topic and the headers (`&'a [(String, String)]`) and owns the payload map that was moved in. Its hand-written `Serialize` writes `type`, `topic`, `partition`, `offset` (as text, through `AsText`) and `headers`, then the payload fields, straight into the output text. `Headers` writes the header pairs as one JSON object. No map is built and no header text is copied.

**Detail**: the old behaviour is kept. A payload field with the name of a fixed field wins, so the fixed field is skipped when the payload has it (the same key never appears twice). A header name that repeats keeps its last value, as it did when the headers went into a map. The key order in the text changes, the old map sorted the keys, and JSON readers do not care. Three new tests in `tests/inbound.rs` check a repeated header, the largest offset and a replaced fixed field, next to the two that existed. The saving is reasoned, not measured: two maps per message and two `String`s per header.

**Why**: a struct that borrows its fields lets serde write the text once, where a `Value` tree forces a copy of every field before it is written.

**Check**: `cargo verify` passes.

**Rule**: see S10.

## I19. Move the payload into the failure context instead of cloning it

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: there is no retry or dead letter path here. A Kafka message whose ACK fails is read again by Kafka (the bridge returns `Outcome::Retry`), and the gateway keeps no failure context.

**Revisit when**: the gateway gets its own retry or dead letter topic.

## I20. Read the payload in place instead of cloning it for serde

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the clone of `serde_json::from_value(payload.clone())` is in the subscriber handlers. The gateway parses the Kafka value once with `parse_inbound` and does not deserialize it a second time.

**Revisit when**: a handler runs `from_value` on a `Value` that it still needs.

## I20.1. Pass the HTTP publish value through as a `RawValue`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: `POST /api/v1/http-publish` reads `value` as a `serde_json::Value` and writes it with `serde_json::to_string(&request.value)`. A `Box<RawValue>` would skip building the tree and forward the exact text the caller sent (serde_json feature `raw_value`). The owner decided on 2026-11-21 to keep the current behaviour: the text in Kafka stays compact JSON with sorted keys, and does not depend on how the caller formatted it.

**Revisit when**: a payload is large enough that building the tree shows up in a measurement, or a consumer needs the exact bytes the caller sent, for example to check a signature. The change is one field type, the `raw_value` feature of `serde_json` and `#[schema(value_type = Object)]` for utoipa.

## I21. Read the status payload in place too

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: see I20. There are no status events in the gateway.

**Revisit when**: see I20.

## I21.1. Move the Kafka value into the inbound record instead of copying it (src/kafka/consumer.rs, bridge.rs)

**Before**: `to_inbound` ran `record.value.map(|value| value.to_vec())`, a full copy of every message value, up to the 10 MB limit (D11). `InboundRecord.value` was `Option<Vec<u8>>`.

**After**: `InboundRecord.value` is `Option<Bytes>` and `to_inbound` moves the value (`value: record.value`). `bytes::Bytes` is the type `kafkit-client` 0.1.9 uses for `ConsumerRecord.value` (checked on docs.rs), and it is re-exported as `axum::body::Bytes`, so no dependency was added. `parse_inbound` still takes `Option<&[u8]>`, because `Option<Bytes>::as_deref()` gives that. The test helper builds the value with `Bytes::copy_from_slice`.

**Detail**: the key of the record is not read, so only the value was copied. The saving is reasoned, not measured: one copy of the message value per Kafka message.

**Why**: a buffer that the client already owns and nothing else needs can be handed on, not copied (S9).

**Check**: `cargo verify` passes.

**Rule**: see S9.

## I22. Set the Azure range in place instead of cloning the event

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: there is no Azure read model and no id range here.

**Revisit when**: the gateway writes a range or any field into a payload it also keeps.

## I22.1. Share one buffer between all subscribers of a frame (src/kafka/subscribers.rs)

**Before**: `SubscriberRegistry::deliver(topic, frame: &str)` ran `frame.to_string()` for every subscriber of the topic, and the queue of a subscriber was `mpsc::Sender<String>`. A frame can be as large as the 10 MB message limit, so ten subscribers meant ten copies.

**After**: `deliver(topic, frame: impl Into<Utf8Bytes>)` makes the shared text once and hands every subscriber a `frame.clone()`. The queues are `mpsc::Sender<Utf8Bytes>` and `Receiver<Utf8Bytes>`. The bridge passes the `String` of `event_frame` by value, which becomes a `Utf8Bytes` without a copy. The socket task sends `Message::Text(text)` as it is. `tests/subscribers.rs` checks that two subscribers get the same buffer (the same address).

**Detail**: `Utf8Bytes` of axum 0.8 wraps `Bytes`, and cloning `Bytes` only counts up a reference (checked in the axum docs: `Clone`, `Deref<Target = str>`, `From<String>` and `From<&str>`). The existing tests did not change their calls: `deliver("orders", "frame")` still compiles, and `as_deref()` on a received frame still gives a `&str`. The saving is reasoned, not measured: for N subscribers, N-1 copies of the frame.

**Why**: one message for many readers is shared, not duplicated.

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I23. start and preview build in release mode before they run

**Before**: `cargo start` was already `run_server("production", true)`: it loads `.env.production`, compiles the CSS, builds the gateway in release mode and then runs it on the port from the env file. `cargo dev` is `run_server("development", false)`. There was no preview profile: `AppEnv` had two values, and there was no `cargo preview` or `cargo build-preview`. `run_build` took no argument and always loaded `.env.production`.

**After**: a preview profile was added, so the three projects have the same commands. `AppEnv` has a third value, `Preview` (`as_str` gives `preview`). `cargo preview` is `run_server("preview", true)`: it loads `.env.preview`, compiles the CSS, builds in release mode and runs. `cargo build-preview` is `run_build("preview")`. `run_build` now takes the profile, validates `.env.{profile}`, compiles the CSS and builds in release mode, without running, and `cargo build-prod` is `run_build("production")`. The usage text in `xtask`, the two aliases in `.cargo/config.toml`, the `APP_ENV` comments, the env error text (I72.1) and the tests in `crates/kafka-env` were changed to match. `.env.preview` is already ignored by git and Docker, because both ignore `.env.*`. D58 in `docs/build.md` records the rule, and `docs/dev-setup.md` and the README list the commands.

**Detail**: the build step runs every time and finishes at once when nothing changed. Since I55 the first release build is slower (fat LTO and one codegen unit), and later ones are as fast as before.

**Why**: the command never stops for a missing build and never runs a stale one, and a preview run uses the same code path as `start`.

**Check**: `cargo verify` passes. `cargo start`, `cargo build-preview` and `cargo preview` were run by hand. `cargo build-preview` printed `Validated .env.preview` and built in 2m 38s, and `cargo preview` then loaded `.env.preview` (`Mode: preview`), built in 0.66s, connected to the database and Kafka, logged `running in preview on http://localhost:8089` and shut down cleanly on Ctrl+C.

## I24. Stop EnvSchema from printing and copying its secrets

**Before**: `EnvSchema` in `src/schemas/env.rs` derived `Clone` and `Debug` while holding the client secret, the JWT secret, the Kafka settings and the database passwords. A stray `println!("{:?}", env())`, a `dbg!` or a failed `unwrap` would have printed all of them.

**After**: `crates/kafka-env/src/schema.rs` derives `Deserialize` and `Validate` only, plus a hand-written `impl fmt::Debug for EnvSchema` that shows nine safe fields (`app_env`, `app_port`, `app_name`, the database server, port and database name, and the Kafka brokers, client id and group id) and ends with `finish_non_exhaustive()`. `AppEnv` keeps its derived `Debug`.

**Detail**: `crates/kafka-env/tests/env.rs` has a test that prints `{:?}` of a full schema and checks that no secret appears. The test values for the secrets are distinct words such as `caller-secret`, so a field name like `kafka_client_id` cannot hide a leak by matching.

**Why**: a new field stays hidden until someone adds it to the list on purpose.

**Check**: `cargo verify` passes.

**Rule**: see S11.

## I25. Remove the unused `dotenvy` dependency from the root package

**Before**: the root `Cargo.toml` listed `dotenvy = "0.15"` and `envy = "0.4"`. Nothing in `src/` or `tests/` used `dotenvy`: only `xtask` loads env files, and it declares its own. `envy` was used only by the env schema.

**After**: `dotenvy` is gone from the root manifest. `envy` moved with the schema into `crates/kafka-env` (I7). `xtask/Cargo.toml` keeps `dotenvy = "0.15"`.

**Detail**: the saving is one small package. The real gain is that the root manifest lists only what its code uses.

**Why**: a dependency nobody uses is compiled, audited and updated for nothing, and it misleads the next reader into looking for where it is used.

**Check**: `cargo verify` passes.

**Rule**: none yet.

## I26. Borrow the client headers and stop the secret from deriving `Debug` and `Clone`

**Before**: `NoAuthHeaders` in `src/schemas/auth.rs` held the client id and the client secret as owned `String`s made with `.trim().to_string()`, and derived `Debug` and `Clone`, although it holds the client secret. The gate and the `NoAuth` extractor both built it, so one API request made up to four heap copies, only to validate and compare.

**After**: `NoAuthHeaders<'a>` holds `&'a str` for both fields and `new` only trims. It derives `Validate` only. `NoAuth` is an empty marker, `pub struct NoAuth;`. The header names are the constants `CLIENT_ID_HEADER` and `CLIENT_SECRET_HEADER`.

**Detail**: the `<'a>` says the two `&str`s live no longer than the request headers they point into. The saving is reasoned, not measured: up to four small allocations and frees per gated API request. The `validator` derive on a struct with a lifetime is one of the points `cargo check` has to confirm.

**Why**: a value that is only read for a moment should be looked at, not copied. A type that holds a secret should not be printable or copyable by accident.

**Check**: `cargo verify` passes.

**Rule**: see S1 and S11.

## I27. Sign tokens from borrowed text with one `Cow` claims struct

**Before**: `sign_token` built the claims with owned `String`s for `sub`, `role` and `iss`, three heap copies per login, only so the library could write them out as JSON.

**After**: `Claims<'a>` in `src/auth/jwt.rs` holds `Cow<'a, str>` for `sub`, `role` and `iss`. `sign_token` fills them with `Cow::Borrowed`, so signing copies nothing. Reading a token gives `Claims<'static>`, which holds owned text read from the token. The test that decodes a token uses `decode::<Claims<'static>>`.

**Detail**: the saving is reasoned, not measured: three small allocations and frees per login, next to a bcrypt check. A second borrowed struct was the alternative. It was rejected because the same four fields would live in two places.

**Why**: one struct serves both directions, and the copy happens only where owned text is really needed.

**Check**: `cargo verify` passes.

**Rule**: see S12.

## I28. Move the password hash out of the user record and stop the records from deriving `Debug` and `Clone`

**Before**: `ApiUserRecord` and `ApiKeyRecord` in `src/db/store.rs` derived `Debug` and `Clone` while holding a password hash and an API key secret hash, and the login cloned the hash to hand it to bcrypt.

**After**: neither record has a derive, with a comment that says why. The password hash is moved out of the record (`verify_password(password, Some(user.password_hash))`) and the API key hash the same way in `check_api_key`. The test fakes keep plain storage structs (`FakeApiUser`, `FakeApiKey`) and build a new record on every lookup, because the records cannot be cloned any more.

**Detail**: the saving is reasoned, not measured: one small heap copy per login and per API key check.

**Why**: the clone was needed only because of a derive that a type holding a secret should not have.

**Check**: `cargo verify` passes.

**Rule**: see S9, S10 and S11.

## I29. Pass the username to the database query as a borrowed `&str`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: already done. The lookups in `src/db/postgres.rs` take the `&str` they receive and bind it, and the `ApiUserStore` and `ApiKeyStore` traits take `&str`.

**Revisit when**: a query function takes a `String` and only binds it.

## I29.1. Pass the refresh token to the database query as a borrowed `&str`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: this entry exists only in jobs-api-gateway-rust. There is no refresh token here.

**Revisit when**: a token or other text is copied before a bind.

## I30. Check a log line without building a `Vec`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: there is no validator that splits a log line and reads its first parts. The WebSocket messages are JSON.

**Revisit when**: a validator runs `split(..).collect()` and reads only the first few parts.

## I30.1. Clean mail text without building a `Vec`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: this entry exists only in jobs-api-gateway-rust. There is no mail text here.

**Revisit when**: text is cleaned with a `Vec` in the middle.

## I31. Lend the correlation id and the job name to the Kafka event instead of cloning

**Before**: the ws-publish session built the Kafka value with `json!`, which clones every field it is given. The correlation id and the job name were cloned for it, and the whole `Value` tree was turned into text with `to_string()`.

**After**: `PublishedEvent<'a>` in `src/features/v1/ws_publish/model.rs` is a `#[derive(Serialize)]` struct with `&'a str` fields (`correlationId`, `jobName`, `timestamp`) and the `apiUserId` number. `PublishedEvent { .. }.to_text()` writes the text once. The ack frame borrows the topic and the correlation id the same way (I65).

**Detail**: the HTTP publish (`http_publish`) passes the caller's `value` through unchanged, so it has nothing to lend (see I20.1). The saving is reasoned, not measured: two clones and one tree per accepted WebSocket message.

**Why**: data that already lives long enough should be lent to a short-lived message, not copied into it.

**Check**: `cargo verify` passes.

**Rule**: see S1 and S10.

## I32. Keep the success message as `&'static str`

**Before**: `SuccessResponse::ok` and `SuccessResponse::accepted` in `src/schemas/response.rs` took a `&str` and the struct stored `message: String` made with `.to_string()`. Every caller passes a fixed text from the source code, or a choice between two fixed texts.

**After**: `message` is `&'static str` and both constructors take `&'static str`, so the text is borrowed and never copied.

**Detail**: a fixed text in the source lives for the whole program, so a `&'static str` is always valid. The saving is reasoned, not measured: one small heap copy per successful response, on every endpoint.

**Why**: fixed text should stay borrowed from the program, not be copied into every reply.

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I33. Keep the health status and version as `&'static str`

**Before**: the app health service built `status: "UP".to_string()` and `version: env!("CARGO_PKG_VERSION").to_string()`. Both texts are fixed when the program is compiled, but the data struct stored them as `String`.

**After**: the fields are `&'static str` and the service assigns the texts directly. `uptime_ms` is a number, and the timestamp changes on every call, so that `String` stays.

**Detail**: the saving is reasoned, not measured: two small heap copies per app health request.

**Why**: fixed text should stay borrowed from the program, not be copied into every reply.

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I34. Borrow the error catalogue text with `Cow`

**Before**: `ApiError::from_entry` ran `.to_string()` on `entry.message` and `entry.error`, two copies of text that already lives for the whole program, on every error response.

**After**: `ApiError` and `ErrorResponse` hold `Cow<'static, str>` for `message` and `error`. `from_entry` uses `Cow::Borrowed`, `new` takes `impl Into<Cow<'static, str>>` for a text built at run time, and `into_response` moves both fields into `ErrorResponse` without a copy. `ApiError::into_message(self) -> Cow<'static, str>` gives the portal login page its text without a copy.

**Detail**: the `utoipa` derive has to accept `Cow<'static, str>` fields as strings (utoipa 6 treats `Cow` as a transparent wrapper, one of the points `cargo check` has to confirm). The saving is reasoned, not measured: two small heap copies per error response.

**Why**: the normal path only needs borrowed text, and owned text is needed only when a caller builds its own, which is what `Cow` is for.

**Check**: `cargo verify` passes.

**Rule**: see S12.

## I35. Render the page into one buffer instead of copying it into a second `String`

**Before**: the render helper called `dioxus_ssr::render(&dom)`, which builds the page body in one `String`, and then wrapped it with `format!("<!DOCTYPE html><html lang=\"en\">{body}</html>")`, a second and larger `String`, on every page view.

**After**: `render_document` in `src/pages/render.rs` starts a `String` with `DOCUMENT_START`, lets `dioxus_ssr::Renderer::new().render_to(&mut html, dom)` write the page into that same `String`, then pushes `DOCUMENT_END`. The page is built once and never copied.

**Detail**: writing into a `String` cannot fail, so a failure there is a bug and `.expect` with that reason is the right choice (S7). The saving is reasoned, not measured: one page-sized allocation and copy per page view.

**Why**: building the front and back of a document around a finished body forces a copy of the body. Writing front, body and back into one buffer in order does not.

**Check**: `cargo verify` passes.

**Rule**: see S2 and S7.

## I36. Keep fixed page texts as `&'static str` in the component props, and `Cow` where kafka builds a text

**Before**: every page copied fixed texts into `String`s on each view: the titles, the About texts and the `Row` labels of the health components, and each component cloned its title to pass it on to `Layout`.

**After**: `LayoutProps.title`, the titles of the page props and the About texts are `&'static str`. `RowProps.label` is `#[props(into)] Cow<'static, str>`, because the Kafka health page has labels built at run time (`Broker 1`, the topic names), which a `&'static str` cannot hold. The fixed labels are borrowed and the built ones are owned. The title constants (`ABOUT_TITLE`, `ABOUT_DESCRIPTION`) live next to the pages.

**Detail**: this is an adaptation of the entry, not a copy. The saving is reasoned, not measured: a handful of tiny heap copies per page view. `#[props(into)]` with a `Cow` is one of the points `cargo check` has to confirm.

**Why**: texts fixed in the source live for the whole program, so borrowing them costs nothing and copying them costs an allocation each time.

**Check**: `cargo verify` passes.

**Rule**: see S1 and S12.

## I37. Build the OpenAPI JSON once instead of on every request

**Before**: the `spec()` handler returned `Json(ApiDoc::openapi())`. Every `GET /openapi/json` rebuilt the whole document and the JSON text, and threw it away after the response.

**After**: a `static SPEC_JSON: LazyLock<String>` in `src/openapi/mod.rs` builds the JSON text on the first request and keeps it. `spec()` returns that text with the `application/json` content type. A failure to turn the spec into JSON is a bug in our own code, so `.expect` with that reason is used (S7).

**Detail**: this is a trade, not a pure gain. The JSON text stays in memory for the whole run. The size was not measured. The speed gain is reasoned, not measured.

**Why**: a document that never changes should be built once, not on every request.

**Check**: `cargo verify` passes.

**Rule**: see S2 and S7.

## I38. Keep the icon class as `&'static str`

**Before**: `IconProps.class` in `src/components/icons.rs` was a `String` with `#[props(default = String::new())]`. Every icon use passes a literal, and Dioxus turned it into a new `String`.

**After**: `class` is `&'static str` with `#[props(default = "")]`. The literal is borrowed and nothing is copied. An icon used without a class still works.

**Detail**: the saving is reasoned, not measured: one tiny heap copy per icon.

**Why**: fixed text should stay borrowed from the program, not be copied into every render.

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I39. Keep one copy of the client header check

**Before**: the client id and secret check was written twice: in the server gate and in the `NoAuth` extractor. Both read the two headers, built `NoAuthHeaders`, ran `validate()` and compared with the env. They differed only in what they did on failure.

**After**: `check_client_headers(headers, expected_id, expected_secret)` in `src/middleware/no_auth.rs` holds the one copy, and `check_client_credentials(headers)` reads the env and calls it (I72). The extractor calls it with `?`. The gate's `has_valid_client_credentials` is `check_client_credentials(headers).is_ok()`. Five unit tests in `no_auth.rs` cover the check.

**Detail**: this is a code-quality change, not a measured saving. Since I34, `ApiError::from_entry` copies no text, so the gate pays nothing extra when a check fails.

**Why**: two copies of a security rule can drift apart, and then the gate and the extractor would disagree about which requests are valid.

**Check**: `cargo verify` passes.

**Rule**: see S8.

## I39.1. Find the auth cookie without building a text for every pair (src/middleware/server_gate.rs)

**Before**: `has_auth_cookie` built a `format!` text for each cookie pair of the request to compare it with the cookie name.

**After**: each pair is cut with `split_once('=')` and compared as a borrowed name and value. Nothing is allocated, and the rule is the same: the cookie `auth=ok` must be found alone and next to others, and `xauth=ok` or `auth=no` must not match.

**Detail**: the saving is reasoned, not measured: one small allocation per cookie pair on every gated browser request. Seven unit tests in `server_gate.rs` cover it (I79).

**Why**: a comparison that only needs a name and a value should split the text it has, not build a new one.

**Check**: `cargo verify` passes.

**Rule**: see S1 and S2.

## I40. Allocate the number text once in `format_number`

**Before**: `format_number` in `src/components/health_parts.rs` started with an empty `String` and pushed one character at a time, so a long number made it allocate again and copy, up to three allocations for a large uptime.

**After**: `String::with_capacity(digits.len() + digits.len() / 3)`. Every third digit adds one comma, so this size always fits and one allocation is enough.

**Superseded**: I61 removes the `String` altogether. `format_number` now returns `impl Display`.

**Why**: when the final size is known before the loop, reserving it once avoids growing and copying inside the loop.

**Check**: `cargo verify` passes.

**Rule**: see S2.

## I41. Skipped: asking tokio for fewer features in the root package

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the root `Cargo.toml` asks for `tokio = { version = "1", features = ["full"] }`. In recordlog-api the same change was tried and reverted, because the Cargo Book says a dependency is built with the union of every feature that any crate in the tree asks for, and the package count did not change. Nothing here suggests a different result, and `cargo tree` cannot be run in the sandbox to measure it.

**Revisit when**: `cargo tree -p kafka-api-gateway --prefix none | sort -u | wc -l` shows a drop with a shorter feature list.

## I42. Move the test response body into the String instead of copying it

**Before**: `body_text` in `tests/common/mod.rs` called `String::from_utf8(bytes.to_vec())`. `to_vec()` always makes a second copy of the whole response body.

**After**: `String::from_utf8(Vec::from(bytes))`. In the locked `bytes` 1.12.1, `Vec::from(Bytes)` reuses the buffer when nothing else holds it, and copies only when the buffer is shared. So it is never worse than before.

**Detail**: this is test code, so it is about avoiding a copy, not speed. Not measured, and I did not check whether axum hands over an unshared buffer in these tests. A reasoned saving only. What every test checks is unchanged.

**Why**: `to_vec()` copies to get an owned buffer. The body is already owned and used once, so it can be moved (S9).

**Check**: `cargo verify` passes.

**Rule**: see S9.

## I43. Keep the answer, not a copy of the header

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: no kafka test copies a header with `.to_string()` only to end a borrow. `header_value` in `tests/common/mod.rs` returns an owned `Option<String>` on purpose and its callers compare it right away with `.as_deref()`. `tests/cors.rs` is new and reads the headers as `&str` from the start.

**Revisit when**: a test copies a header only to end a borrow.

## I44. Use a fixed array for the authorization test headers

**Before**: the helpers that add the API key headers to the two client headers called `auth_headers().to_vec()` and then `push` for each extra header. That put the two client headers on the heap in a `Vec`, then grew it.

**After**: `let [client_id, client_secret] = auth_headers();` and one array on the stack, for example `[client_id, client_secret, ("x-api-key", key), ("x-api-secret", secret)]`. The helpers return `[(&str, &str); 4]` and the tests pass `&...` as a slice. It applies to eight places in three test files: `api_key_headers` and the inline list in `tests/topics_reload.rs`, `publish_headers` and one inline list in `tests/http_publish.rs`, and `probe_headers` and three inline lists in `tests/api_auth.rs`. The two lists of cases in `api_auth.rs` are `[&[(&str, &str)]; 2]` arrays of slices.

**Detail**: test code, so this is clarity plus one or two heap allocations per request, not speed. Reasoned, not measured. The requests that are sent are the same. The two `post_*` helpers in `tests/common/mod.rs` take a caller-supplied slice of unknown length, so they stay as slices.

**Why**: a `Vec` is for a list that can grow. When the length is known at compile time, an array says so and needs no heap (S1).

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I45. Use a fixed array for the CORS test headers

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka had no CORS test before this pass, so there was no `vec![("origin", origin)]` with `extend(auth_headers())` to change. The `tests/cors.rs` that I57 added already builds `[("origin", origin), client_id, client_secret]` as a fixed array from its first line.

**Revisit when**: a test builds a header list with `vec!` and `extend` for a length that is known at compile time.

## I46. Use a fixed array for the gate test headers

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the tests in `tests/gate.rs` and `tests/kafka_health.rs` already use constants and fixed arrays for their header lists (`BROWSER` in both files, `JSON` in `kafka_health.rs`). No `vec!` followed by `extend` exists there.

**Revisit when**: a gate test builds a header list with `vec!` and `extend` for a length that is known at compile time.

## I47. One request-sending helper for the test requests

**Before**: `tests/common/mod.rs` had two functions with the same header loop, `send` (own router, no body) and `send_to` (given router and a body). `post_json_to` and `post_raw_to` each did `headers.to_vec()`, pushed the `content-type` header on the copy and passed the longer slice on.

**After**: one private `send_to(app, request: Builder, headers, body)` holds the header loop, the router call and the send. `get`, `post`, `get_to` and `post_raw_to` only start the request. `post_raw_to` puts `content-type` on the builder, so the copy of the header list is gone. `post_json_to` and `post_form_to` call `post_raw_to`.

**Detail**: test code, so this is less duplication and one `Vec` less per request, not speed. Not measured. The body is still copied with `body.to_string()`, because `Body` must own its bytes and the argument is a borrowed `&str`. No test changed what it checks.

**Why**: the loop that sets the headers is written once, so a change to it (another header rule, a different send) is one edit (S8).

**Check**: `cargo verify` passes.

**Rule**: see S8.

## I48. Write the Bearer header line once in the extractor tests

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the extractor tests in `tests/api_auth.rs` already share one helper, `bearer_headers`, that builds the `authorization` header from a token. The `format!("Bearer {token}")` text is written in that one place.

**Revisit when**: a second test file repeats the Bearer line.

## I49. Use the shared test client id constant instead of plain text

**Before**: six test files (`api_auth.rs`, `errors.rs`, `gate.rs`, `health.rs`, `http_publish.rs`, `topics_reload.rs`) wrote the client id as the plain text `"test-client"` in nine places, and `api_auth.rs` and `errors.rs` wrote the secret as `"test-secret"` in three places, while `tests/common/mod.rs` already holds both values as `TEST_CLIENT_ID` and `TEST_CLIENT_SECRET`.

**After**: those files import the two constants and use them. The `"wrong"` secrets stay as plain text, because they are meant to be wrong. The new `tests/ws_socket.rs` uses the constants from its first line.

**Detail**: this is about duplication and clarity only. Not measured. No test changed what it checks.

**Why**: the id and secret the tests configure and the ones they send now come from one place. If a constant changes, the tests cannot silently stop matching it (S8).

**Check**: `cargo verify` passes.

**Rule**: see S8.

## I50. Choose the TLS provider in the subscriber package

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the item fixes a crash that came from moving a subscriber into its own package (I16), after which nothing in that package enabled a rustls provider any more. Kafka has one server package and no such split, and this pass did not change which crate provides TLS for it. I could not run the Docker image here, so a missing provider would show only at the first start.

**Revisit when**: the container log shows `Could not automatically determine the process-level CryptoProvider`, or a second package that opens TLS connections is added.

## I51. One logging crate for the API and the subscriber

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has one server binary and one logger file, `src/logging.rs`, with one line format. There is no second package that needs the logger, so a crate would only add a manifest and a path dependency.

**Revisit when**: a second package that runs for a long time needs the same log line format.

## I51.1. Test the timestamp in the logging module

**Before**: `src/logging.rs` had no test (`cargo test` ran 0 tests for it), although every log line starts with `iso_now_ist()`.

**After**: `src/logging.rs` has a unit test that checks the shape of `iso_now_ist()`: the text is 29 characters long, ends with `+05:30`, and has `T` at byte 10 and `.` at byte 19.

**Detail**: test only, no behavior change. The test reads the clock, but it checks only the shape, so it cannot fail by the time of day. The reference project has this test in its logging crate, kafka has the function in one file, so the test sits in that file.

**Why**: one timestamp function stamps every line, so a change of its format should fail a test.

**Check**: `cargo verify` passes.

**Rule**: see S8.

## I52. Move the subscriber into crates/

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no subscriber package. `crates/kafka-env` is already in `crates/` (I7), and `xtask/` stays in the root, because the cargo-xtask README shows it there and `compose_file()` finds `docker/docker-compose.yml` from `CARGO_MANIFEST_DIR` with fixed `..` steps, so a move would break every `cargo docker-*` command.

**Revisit when**: a second runnable package is added.

## I53. Publish the log lines in chunks of 100

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka does not publish log lines or mails in chunks. A publish request carries one value and makes one Kafka record, in `src/features/v1/http_publish` and `ws_publish`.

**Revisit when**: a publish path that splits one input into several records is added.

## I54. Apply I35 in the code, and fix a duplicate comment and a typo

**Before**: the page helper wrapped the rendered body in a second `String` with `format!`. That is the copy that I35 describes. In recordlog-api, I35 was written in the doc but the code was never changed, and I54 is the pass that changed it.

**After**: kafka applies I35 in the code in the same pass (`render_document` in `src/pages/render.rs`), so I54 adds no separate change. `render.rs` has no leftover `wrap` function, and I found no duplicate comment or typo of the kind that was fixed in recordlog-api, so nothing else was changed.

**Detail**: this entry records that the check was done, so the number stays in place.

**Why**: a rule that is written in a doc must also be in the code, so the doc and the code do not disagree.

**Check**: `cargo verify` passes.

**Rule**: none.

## I55. Tune the release profile for speed and a small binary

**Before**: `Cargo.toml` had no `[profile.release]`. The Cargo Book lists the release defaults as `lto = false`, 16 codegen units and `strip = "none"`.

**After**: `[profile.release]` sets `codegen-units = 1`, `lto = "fat"` and `strip = "symbols"`. `panic` stays `"unwind"` on purpose: `CatchPanicLayer` turns a handler panic into a 500, and the Rust Performance Book says `panic = "abort"` is only for programs that do not need to unwind.

**Detail**: the Rust Performance Book names `codegen-units = 1`, `lto = "fat"` and `strip = "symbols"` among the options for speed and binary size. Release builds get slower, so `cargo start` (I23) and the Docker build (I67) take longer. `cargo dev` uses the dev profile and is not affected. The gain was not measured, because no registry is reachable here, so compare binary size and `cargo build --release` time before and after.

**Why**: these three settings cost build time and nothing else, and every request runs the resulting code.

**Check**: `cargo verify` passes.

**Rule**: none, this is a build setting.

## I56. Let browsers reuse the static files for one hour

**Before**: `/global.css`, `/logo.png` and `/favicon.ico` were sent with no `Cache-Control`, so every page view could download them again.

**After**: `STATIC_CACHE_CONTROL` in `src/app.rs` is `public, max-age=3600`, and all three handlers send it. `static_files_are_cacheable_for_one_hour` in `tests/gate.rs` checks the header on all three.

**Detail**: MDN and the Lighthouse guidance only recommend `immutable` with a one year `max-age` for URLs whose name changes when the content changes. These URLs have no content hash, so a long cache would keep an old stylesheet after a deploy. One hour is the compromise.

**Why**: a file that is already in the browser cache costs no request at all.

**Check**: `cargo verify` passes.

**Rule**: none.

## I57. Let browsers reuse the CORS preflight answer for one hour

**Before**: `cors_layer` in `src/middleware/cors.rs` sent no `Access-Control-Max-Age`, so browsers fall back to a short default and ask with an OPTIONS request again and again. Kafka had no CORS test at all.

**After**: `.max_age(PREFLIGHT_MAX_AGE)` with `Duration::from_secs(3600)`. `tests/cors.rs` is new and has seven tests: a fixed origin, a suffix origin, an unknown origin, a request without an origin, a preflight from an allowed origin, a preflight from an unknown origin, and the `Access-Control-Max-Age` value `3600`.

**Detail**: the `tower-http` source shows `max_age` takes anything that converts into `MaxAge`, and `Duration` does. The owner then added `x-api-key` and `x-api-secret` to `allow_headers`, which now lists `content-type`, `authorization`, `x-client-id`, `x-client-secret`, `x-api-key` and `x-api-secret`. Before that, a browser that called the API-key routes (`/api/v1/http-publish`) with those headers would have failed its preflight. Server-to-server callers were never affected. `tests/cors.rs` does not assert the two new names yet.

**Why**: every skipped preflight is one request fewer before the real call.

**Check**: `cargo verify` passes.

**Rule**: none.

## I58. Compare the client id and secret in constant time

**Before**: `check_client_credentials` in `src/middleware/no_auth.rs` compared the sent id and secret with `!=`, which stops at the first byte that differs, so the time of a wrong answer can reveal how much of the secret was right.

**After**: both pairs are compared with `subtle::ConstantTimeEq` on the bytes, and the two results are joined with a single `&`, not `&&`, so the secret is compared even when the id is wrong. A mismatch still returns the same 400 `INVALID_HEADER_VALUES` error. `subtle` is a direct dependency of the root package.

**Detail**: `ct_eq` on slices of different length returns false at once, which only shows the length, and the length of the id and the secret is limited by the header validation anyway. The saving is security, not speed. `subtle` 2.6.1 was already in `Cargo.lock` as a dependency of other crates, so the lock file only gains the edge from this package.

**Why**: a secret should not be compared with an operation whose time depends on the secret.

**Check**: `cargo verify` passes.

**Rule**: see S15.

## I59. Render the fixed pages once

**Before**: `GET /`, `GET /about` and `GET /login` built a `VirtualDom` and rendered it on every request, although the output never changes.

**After**: `home.rs`, `about.rs` and `login.rs` in `src/pages/` each have a `static ...: LazyLock<String>` (`HOME_HTML`, `ABOUT_HTML`, `LOGIN_HTML`) that renders the page on first use. The handler returns `Html(&'static str)`, so no render and no copy happens per request. A login page with an error message is still rendered per request, because its text changes.

**Detail**: the `dioxus-ssr` docs say a `Renderer` keeps its template cache, but `render_document` makes a new `Renderer` per page, so nothing is reused. The health pages are not cached because their uptime and timestamp change. The cost is three small strings that stay in memory.

**Why**: work whose result never changes should be done once.

**Check**: `cargo verify` passes.

**Rule**: see S16.

## I60. Serve the Scalar page from one cached `String`

**Before**: `Scalar::with_url("/openapi", ApiDoc::openapi())` in `utoipa-scalar` 0.4 builds the html when the router is made (at start-up), and its axum route clones that whole text, spec included, for every request.

**After**: `SCALAR_HTML` in `src/openapi/mod.rs` is a `LazyLock<String>` built with `Scalar::new(...).title(...).to_html()` on the first request, and `scalar()` returns `Html(&'static str)`. The `Servable` import and the `.merge(...)` are gone. This also moves the one-time build from start-up to the first request.

**Detail**: the `axum` feature of `utoipa-scalar` is no longer used by the code, but it was left in `Cargo.toml` so `Cargo.lock` does not change. I could not compile this against the real crate here, so the first `cargo check` is what confirms that `Scalar::new`, `title` and `to_html` have these signatures in 0.4.

**Why**: one copy of the page held for the whole program is cheaper than one new copy per request.

**Check**: `cargo verify` passes.

**Rule**: see S16.

## I61. Print the health numbers into the page without temporary `String`s

**Before**: `format_number` built a `String` of digits and a second `String` for the result, and `format_latency` allocated a `String` for the dash. Dioxus then copied them into the page.

**After**: both functions return `impl Display`. `Grouped` in `src/components/health_parts.rs` keeps up to 20 digits in a stack array and writes them with commas straight into the page buffer, and `Latency` writes the number or the dash. The call sites did not change. `tests/format.rs` checks 0, 999, 1,000, 12,345, 123,456, 1,234,567 and `u64::MAX`, and the dash and a number for the latency. `format_bytes` and `format_ago` are unchanged, because they format floats and are not on the hot path.

**Detail**: before the doc was written, I compared the new digit loop with the old function in a small std-only program on 200,251 numbers (0 to 199,999, every power of ten and of two and their neighbours, and the largest values), and every result was the same. The kafka page also prints the broker count through `format_latency`.

**Why**: a value that only exists to be printed once does not need a heap copy.

**Check**: `cargo verify` passes.

**Rule**: see S2.

## I62. One password check for both logins, and it logs the cause

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has one `verify_password` in `src/auth/password.rs`, used by the token login, the portal login page and the API key check. It runs bcrypt on a blocking thread, checks a dummy hash when the user does not exist, and logs the real failure before it returns the generic server error.

**Revisit when**: a second password check is added.

## I63. Build the JWT header and rules once

**Before**: every `verify_token` call ran `Validation::new`, `set_issuer`, `set_required_spec_claims` and `DecodingKey::from_secret`, and every `sign_token` call ran `Header::new` and `EncodingKey::from_secret`.

**After**: `src/auth/jwt.rs` has `HEADER` and `VALIDATION` as `LazyLock` statics, and both functions use them. The two keys are still built per call, because kafka passes the secret in as an argument (S14), so a cached key would have to read the global env.

**Detail**: `Validation::new`, `set_issuer`, `set_required_spec_claims` and `Header::new` build sets and `String`s on every call. I did not read the source of `jsonwebtoken` 11 here, so I give no count of allocations, and the saving is reasoned, not measured. The copy of the secret that each key makes remains, one per call. This is the partial form of the item on purpose.

**Why**: the header and the rules are the same for every request.

**Check**: `cargo verify` passes.

**Rule**: see S16 and S14.

## I64. Declare shared dependencies once in the workspace

**Before**: the root package and the new crates would have written `serde`, `serde_json`, `validator`, `bcrypt` and the `kafka-env` path in each manifest on its own.

**After**: the root `Cargo.toml` has `[workspace.dependencies]` for `kafka-env`, `bcrypt`, `serde`, `serde_json` and `validator`, and the root package, `crates/kafka-env` and `xtask` use `{ workspace = true }`. The root package has no `[workspace.package]`, because `semantic-release-cargo` edits the version in `Cargo.toml`.

**Detail**: I parsed all three manifests with a small script and checked that every `workspace = true` name is in the workspace table: all ten uses matched. `Cargo.lock` was edited by hand for the new `kafka-env` package and for the dev-dependencies of I73.1, so the first `cargo check` without `--locked` may rewrite it. That is expected.

**Why**: one place to change a version or a feature, so crates cannot drift apart.

**Check**: `cargo verify` passes.

**Rule**: see S17.

## I65. Build the socket frames with a borrowing struct instead of a `Map` and `json!`

**Before**: the WebSocket handlers built every frame with `json!({ ... })` and passed it to `frame(fields: Value) -> String` in `src/ws/mod.rs`, which called `fields.to_string()`. `json!` builds a `Value` tree, and `json!(value)` clones the value it is given, because it serializes a reference.

**After**: `Frame<'a>` in `src/ws/frames.rs` is one `#[derive(Serialize)]` enum with `tag = "type"`, `rename_all = "kebab-case"` and `rename_all_fields = "camelCase"`. Its fields are borrowed (`&'a str`) and `Option` fields use `skip_serializing_if`, so they are left out when missing. `serde_json::to_string(&frame)` writes straight into the output text. No map is built and no field is cloned. The helper functions `auth_error_frame` and `error_frame` build the text for the error frames of the publish and subscribe handlers.

**Detail**: the saving is reasoned, not measured: one `Value` tree, its key `String`s and the field clones for each frame. The key order in the text can change, which JSON readers do not care about. The tests in `tests/ws_publish.rs` and `tests/ws_subscribe.rs` compare the parsed JSON, not the text, so they do not depend on the order. `rename_all_fields` needs serde 1.0.183 or newer, and the lock file has 1.0.229, but this derive could not be compiled here, so the first `cargo check` is what confirms it.

**Why**: a struct that borrows its fields lets serde write the text once, where a `Value` tree forces a copy of every field before it is written.

**Check**: `cargo verify` passes.

**Rule**: see S10.

## I65.1. Keep the `Cow` in `clean_text` so clean text is not copied

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the item belongs to the mail reader of the jobs project (I30.1), and kafka has no mail reader and no `clean_text`.

**Revisit when**: kafka reads text that has to be cleaned with a regex.

## I66. Borrow the event texts from the payload in the subscriber

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no subscriber and no typed event that is parsed from a payload. A Kafka record becomes one socket frame, and that path already borrows what it can (I18.1, I21.1, I22.1).

**Revisit when**: kafka parses an incoming payload into a typed struct that is read once and dropped.

## I67. Build the Docker image from a cache, locked, and run it as a normal user

**Before**: `docker/Dockerfile` ran a plain `cargo build --release`, so every image build downloaded and compiled all dependencies again, even for a one line change. The build did not use `--locked`, so a stale `Cargo.lock` could be changed silently, and the container ran as root.

**After**: the build step uses BuildKit cache mounts for `/usr/local/cargo/registry` and `/app/target`, runs `cargo build --release --locked --package kafka-api-gateway`, and copies the binary out in the same `RUN`, because a cache mount is not part of the image. The new workspace needs `crates/` and `xtask/` in the build context, so the builder stage copies them next to `Cargo.toml`, `Cargo.lock`, `src` and `public`. The final stage creates the system user `app` (uid 10001, no home, no login shell) and runs the binary as that user. `docker/docker-compose.yml` is unchanged.

**Detail**: BuildKit is the default builder since Docker 23.0. The server writes no files (I searched `src` and `crates` for file writes and found none), and the three static files are compiled into the binary with `include_str!` and `include_bytes!`, so the unprivileged user needs no writable folder. `.dockerignore` already leaves out `target`, `.git` and every `.env*` file, and the compose file passes the env at run time. The first build after this change is as slow as before, and later builds are faster. The size of the gain was not measured. The image was not built where this was written (no Docker, no network). The owner then built it with `cargo docker-build` and started it with `cargo docker-up`, and it ran correctly.

**Why**: a build that reuses its work is faster to repeat, `--locked` makes the image match the lock file, and a process that does not need root should not have it.

**Check**: `cargo verify` does not build the image. `cargo docker-build` and `cargo docker-up` are the check, and both were run by hand and worked.

**Rule**: none.

## I68. Write any value straight into the publish frame, and send status events as a struct

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the item changes the publish request of the subscriber transport (`PublishRequest`, `publish_status`, the retry and dead letter headers). Kafka has none of these. Its publish path is `http_publish` and `ws_publish`, which hand one value to the Kafka producer. Passing that value through as a `RawValue` was considered and declined as I20.1.

**Revisit when**: kafka builds frames that carry a list of header pairs or a status event for every message.

## I68.1. Lend the payload to the next stage instead of copying it

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: the item belongs to the jobs project (`with_azure_write`, a view of the payload that adds `azureWrite`). Kafka has no stage that edits a payload and passes it on.

**Revisit when**: a stage has to add a field to a payload that is also kept for a retry.

## I69. Borrow the failure context and lend the event to the failure path

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no retry, dead letter or status path for an event. A failed publish returns its error to the caller, and a failed delivery to a socket is logged and dropped.

**Revisit when**: kafka gets a retry or dead letter path.

## I70. Name the stage and status with `as_str`, not with a serde round trip

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no pipeline stage or status enum that is turned into text through serde. `AppEnv::as_str` already is the `as_str` form of this item (I78).

**Revisit when**: an enum with fixed names is turned into text with `serde_json::to_value`.

## I71. Cut a text with a borrowed slice, not with `chars().take(n).collect()`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: no kafka code cuts a text to a number of characters with `chars().take(n).collect()`.

**Revisit when**: a text has to be cut to a number of characters, use a slice found with `char_indices().nth(n)`.

## I72. Read the env at the edge and pass plain values in

**Before**: `check_client_credentials` in `src/middleware/no_auth.rs` read `env()` inside the check, so a unit test of the check needed the global env, and the check could not be called with other values.

**After**: `check_client_headers(headers, expected_id, expected_secret)` takes the two expected values as arguments (I39), and `check_client_credentials(headers)` is the one place that reads `env()` for them. The auth handler passes `&env().jwt_secret` into `login`, and the auth middleware passes it into `verify_token`, so those functions already took the secret as a value. The five unit tests of `check_client_headers` run with plain strings and without any env variable.

**Detail**: this is S14, applied to the client credentials. The rest of I72 (`Topics`, `GatewayConfig`, `Backoff` and gateway log lines that name only the frame type) belongs to the subscriber, which kafka does not have.

**Why**: values read once at the edge are plain arguments, and the code that uses them can be tested without global state.

**Check**: `cargo verify` passes.

**Rule**: see S14.

## I72.1. Print an env error with the variable name only, in the server and in xtask

**Before**: `EnvSchema::load` returned the text of `envy` as it came. For a value that cannot be parsed, that text holds the value (`... while parsing value '<the value>' provided by app_port`). Both users of the schema printed it as it was: the server in its `ENV` static, and xtask in `load_env` for the `.env.<profile>` file.

**After**: `from_pairs` in `crates/kafka-env` rewrites the error (`describe_envy_error`), and both callers print the rewritten text: `X is required`, `X has an invalid value`, or `APP_ENV must be development, preview or production`. One rewrite covers the server and xtask, because both use the same schema crate (I7). The validator messages already named the variable and do not hold the value.

**Detail**: kafka only, because the xtask here reads the real `.env.development` and `.env.production` files, which hold the database and API secrets, and prints their errors to a terminal and to a log. The rewrite itself and its tests are written up in I78.

**Why**: a start-up error is the one message that is always read, so it must not hold a secret.

**Check**: `cargo verify` passes.

**Rule**: see S22.

## I73. Test the subscriber against a fake gateway

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no subscriber and no gateway client. The same idea, a real socket on a local port, is applied to the WebSocket routes as I73.1.

**Revisit when**: kafka gets a client that talks to another service over a socket.

## I73.1. Test the WebSocket routes over a real socket

**Before**: the WebSocket tests (`tests/ws_publish.rs`, `tests/ws_subscribe.rs`, `tests/bridge.rs`) run the session and bridge code in memory. None of them did a real upgrade, so nothing checked the handshake, the client headers on it, the frames as a client reads them, the close codes, or the shutdown.

**After**: `tests/ws_socket.rs` starts the real router on `127.0.0.1:0` with `axum::serve` and connects with `tokio-tungstenite`. The Kafka side is the in-memory fakes, so no Kafka and no database are needed. Six tests: a publish connection answers `ready`, publishes and answers with an ack; a message that is not JSON gets an error frame and the connection stays open; a wrong API secret gets an error frame and the close code 4001; a subscribe connection without a subscriber id is refused with 400 before the upgrade; one delivered frame reaches every subscriber of the topic through its own socket; a shutdown closes an open connection with 1001. Every wait has a timeout, so a broken test fails and does not hang.

**Detail**: `tokio-tungstenite` 0.29 and `futures-util` 0.3 are dev-dependencies. Both are already in `Cargo.lock` (tungstenite 0.29.0 and futures-util 0.3.34), and I added the two names to the dependency list of the kafka package in `Cargo.lock` by hand, so the first `cargo check` without `--locked` may rewrite that file. These tests were not run here: the handshake through the router with the client headers, and the timing, are what the first `cargo test` must confirm.

**Why**: the code that decides what a client sees on the wire is best tested through a socket, and a socket on the local machine needs nothing else.

**Check**: `cargo verify` passes.

**Rule**: see S21.

## I74. Run one bcrypt check for an unknown user too

**Before**: `login` in `src/features/v1/auth/service.rs` and `login_submit` in `src/pages/login.rs` returned at once when the user did not exist, and ran bcrypt, which is slow on purpose, only for a user that did. The answer was the same text for both cases, but the response time told an attacker which user names exist.

**After**: `verify_password(password: String, hash: Option<String>)` in `src/auth/password.rs` checks a missing hash against `DUMMY_HASH`, a valid cost 12 bcrypt hash of a random password nobody knows, and returns `false` for it whatever the password. Both logins call it once and decide from the result. The answers are the same as before (`INVALID_CREDENTIALS` for an unknown user and for a wrong password). The password and the hash move into the blocking thread, so nothing is copied for it. `tests/password.rs` checks a match, a mismatch, a missing hash that never matches, and that a broken hash is a server error.

**Detail**: an unknown user now costs one bcrypt check, so a flood of unknown names costs CPU like a flood of wrong passwords. Rate limiting is not part of this change. `DUMMY_HASH` is the same string as in the other projects and it is a 60 character `$2b$12$` hash. I could not run bcrypt where this was written, so `tests/password.rs` is what proves that it parses (a bad dummy hash would make the missing-hash test return a server error). Those tests passed in `cargo verify`.

**Why**: the time a login takes must not depend on whether the user exists.

**Check**: `cargo verify` passes.

**Rule**: see S19.

## I74.1. Pay for one bcrypt check when an API key is refused

**Before**: `verify_api_key_secret` in `src/middleware/api_key.rs` returned at once when the store refused the key (unknown, inactive or expired), and ran bcrypt only for a key that existed. The refusal was fast and the wrong secret was slow, so the time told which key ids exist.

**After**: when the store answers with `ErrorCode::Unauthorized`, the middleware runs `verify_password(secret.to_string(), None)` first, which checks the dummy hash, and then returns the same error. A database failure still answers at once, because it says nothing about the key. `api_key_auth_with_unknown_key_takes_as_long_as_a_bcrypt_check` in `tests/api_auth.rs` asserts that a refused key takes at least 50 ms.

**Detail**: the test checks only a lower bound. A bcrypt check at cost 12 never takes less than 50 ms, so the test cannot fail by a slow machine, and it does not prove the two times are equal. An unknown key now costs CPU like a wrong secret, and the same note about rate limiting applies. The owner decided to apply this.

**Why**: a refusal and a wrong secret should take the same time, so the answer does not show which key ids exist.

**Check**: `cargo verify` passes.

**Rule**: see S19.

## I75. Leave Debug and Clone off the two login types

**Before**: `LoginRequest` (`src/features/v1/auth/model.rs`) derived `Debug`, and `LoginForm` (`src/pages/login.rs`) derived `Debug`. Both hold the password, so a `{:?}` in a log line or a `dbg!` would print it.

**After**: both derive only `Deserialize` and `Validate` (and `ToSchema` for the request), with a comment that says why. Neither derived `Clone`, and nothing needs one.

**Why**: a type that holds a password should not be printable or copyable by accident.

**Check**: `cargo verify` passes.

**Rule**: see S11.

## I76. Convert numbers without `as` where a value can be out of range

**Before**: `src/features/v1/auth/service.rs` turned the token expiry into the answer with `signed.expires_at as i64`, and `src/features/v1/health/service.rs` used `as_millis() as u64` for the uptime. `as` wraps silently, so a value beyond the target type would turn into a wrong number.

**After**: `i64::try_from(signed.expires_at)` goes through `.ok().and_then(..)` into the existing `TOKEN_SIGNING_FAILED` error, so a value out of range is an error, not a wrong date. `millis(Duration) -> u64` in the health service caps at `u64::MAX` with `u64::try_from(..).unwrap_or(u64::MAX)`, and the uptime uses it, with two tests (whole milliseconds, and `Duration::MAX` is capped). The digit cast in `health_parts.rs` (`rest % 10` as `u8`) stays, with a comment that it is always below 10.

**Detail**: `src/kafka/activity.rs` and `src/kafka/health_probe.rs` already used `try_from` with a cap, so they are not changed.

**Why**: a conversion that can lose data should say what happens when it does.

**Check**: `cargo verify` passes.

**Rule**: see S20.

## I77. Log the panic text from the payload, without copying it

**Before**: `handle_panic` in `src/errors/panic.rs` turned the panic payload into an owned `String` (`to_string()` and `clone()`) before logging it.

**After**: it borrows the text: `downcast_ref::<&str>().copied()`, then `downcast_ref::<String>().map(String::as_str)`, then a fixed fallback. The text is logged and never sent to the client, as before.

**Detail**: a panic is rare, so this saves almost nothing at run time. It is a shorter function, not a faster one.

**Why**: a text that is only read for a log line is lent, not copied.

**Check**: `cargo verify` passes.

**Rule**: see S1.

## I78. Report a bad env variable by name, never by value, and test the schema

**Before**: `EnvSchema::load` returned `envy`'s own error text (see I72.1), and nothing tested the schema. `trim_fields` rebuilt every text value with `trim().to_string()`, even when there was nothing to cut off.

**After**: `EnvSchema::from_pairs` builds the schema from name and value pairs (`load` calls it with `std::env::vars()`), and `describe_envy_error` rewrites the error as described in I72.1. `trim_fields` copies a value only when something was cut off. `crates/kafka-env/tests/env.rs` has 12 tests: a valid set, the default `ONPREM_DB_PORT`, a missing variable, `APP_ENV` with each of its three values, a wrong value and its text, number ranges, a non-numeric port, the length rules, trimming before the check, that no secret value appears in an error text, that an unparsable value is not echoed, and that `Debug` leaves the secrets out. Four unit tests in `src/env.rs` cover the error text.

**Detail**: `AppEnv::as_str` already existed. The `unknown variant` rewrite is fixed text, because `envy` gives no variable name for a wrong enum value and `APP_ENV` is the only enum. The tests were not run where this was written. They passed in `cargo verify`.

**Why**: a start-up error is the one message that is always read, so it must not hold secrets, and a rule like that stays true only if a test checks it.

**Check**: `cargo verify` passes.

**Rule**: see S22.

## I79. Test the helpers of the server gate

**Before**: `src/middleware/server_gate.rs` had no unit tests, although it decides which requests are served before a login: the cookie check, the file extension check and the WebSocket upgrade check.

**After**: seven unit tests: the auth cookie is found at any place among other cookies and in two `Cookie` headers; a wrong value, a longer name, a longer value, an empty value or no cookie is not the login cookie; the cookie that the login sends is the cookie the gate looks for; paths that end in a file extension are static files; API paths, an empty extension and a dot in a folder name are not; an `Upgrade: websocket` header is found in any letter case; another value or no header is not an upgrade.

**Detail**: the tests call the small helper functions (`has_auth_cookie`, `has_file_extension`, `is_websocket_upgrade`, `auth_cookie_header`), not the middleware. The middleware itself is covered by `tests/gate.rs`.

**Why**: a gate that decides what is served without a login should have its rules written down as tests.

**Check**: `cargo verify` passes.

**Rule**: none.

## I80. Return nothing from `post_to_gateway_at`

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no gateway client and no `post_to_gateway_at`.

**Revisit when**: a test helper returns a value that no caller reads.

## I81. Give the Azure pool numbers a name

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no Azure pool. Its PostgreSQL pool numbers are already named constants with a comment each.

**Revisit when**: a pool or a timeout gets a bare number.

## I82. Pass the Azure id range to the read-model procedure

**Skipped in kafka-api-gateway-rust.**

**Why it does not apply**: kafka has no read-model procedure and no Azure database.

**Revisit when**: kafka calls a database procedure that takes a range.

## Summary of the kafka pass (2026-11-21)

- Of the 89 numbered items in the other two projects (I1 to I82 and the seven jobs-only sub-numbers), 52 are applied here and 37 are skipped with a reason. Kafka adds its own sub-numbers: I18.1, I21.1, I22.1, I39.1, I72.1, I73.1 and I74.1 (applied), and I20.1 (skipped by decision).
- This pass was written without compiling (no network, no registry), with every changed file formatted by `rustfmt --edition 2024` and read line by line. It was then checked on the owner's machine, where `cargo verify` passed. `Cargo.lock` was edited by hand: for the new `kafka-env` package and for the two dev-dependencies of I73.1. The first `cargo check` without `--locked` may rewrite it.
- Allocations removed, all reasoned and none measured: the argument, line and colour copies in xtask (I1 to I5), a full copy of every event frame and of its headers (I18.1), a copy of every Kafka value (I21.1), one buffer instead of one copy per subscriber for every frame (I22.1), the page-sized copy per page view (I35), the pages and the OpenAPI and Scalar documents built once instead of per request (I37, I59, I60), the header and rules of every token (I63), the temporary digit and dash `String`s in the health pages (I61), a `Value` tree and its field clones per socket frame (I65), the owned panic text (I77), and a second copy of every test response body and a heap `Vec` for the test headers (I42, I44, tests only).
- Secrets: the env type, the client headers, the login types and the API user no longer print or copy their secrets (I24, I26, I28, I75). The client id and secret are compared in constant time (I58). An unknown user and a refused API key cost one bcrypt check like a wrong password (I74, I74.1). An env error names the variable and never the value (I72.1, I78).
- Tests: `crates/kafka-env` has 16 tests (12 in `tests/env.rs` and 4 unit tests). New test files: `tests/cors.rs` (7 tests, I57), `tests/ws_socket.rs` (6 tests, I73.1), `tests/format.rs` (I61) and `tests/password.rs` (I74). New unit tests: 7 for the server gate (I79), 5 for the client credential check (I72) and 1 for the log timestamp (I51.1). Added to existing files: 3 for `event_frame` (I18.1), 1 for a shared buffer (I22.1), 1 for static file caching (I56), 1 for the API key timing (I74.1) and 2 for the health `millis` (I76).
- Running the app: `cargo start` and `cargo preview` build in release mode before they run, so they never run an old build, and `cargo build-preview` builds without running (I23). `APP_ENV` accepts `development`, `preview` and `production` (I7, I72.1). The release profile uses one codegen unit, fat LTO and stripped symbols, so `cargo start` and the Docker build are slower, and the gain was not measured (I55). Browsers keep the static files and the CORS preflight answer for an hour (I56, I57). The Docker build reuses its cache, uses `--locked` and runs as a normal user (I67). The owner built and ran the image.
- Skipped on purpose: forwarding the HTTP publish value as a `RawValue` (I20.1), because the owner wants today's compact, key-sorted JSON. Skipped because kafka has a single runnable package and no shell string: I10, I12, I14, I16, I50, I51 and I52. Skipped because kafka has no subscriber, retry path, Azure database or mail reader: I15.1, I15.2, I17, I18 to I22, I29.1, I30, I30.1, I53, I65.1, I66, I68 to I71, I73 and I80 to I82. Skipped because kafka already has the fix or never had the pattern: I29, I43, I45, I46, I48 and I62. I41 is skipped as in the other projects, because tokio features are a union across the tree.
- The CORS `allow_headers` list now also has `x-api-key` and `x-api-secret`, so a browser that calls the API key routes passes its preflight (I57).

## Applied in

- recordlog-api: I1 to I15.
- jobs-api-gateway-rust: I1 to I21, I23 and I24 (2026-11-13 and 2026-11-14), I22 skipped. I26 to I29, I31 to I34 and the jobs-only I29.1 and I30.1 (2026-11-15), I25 and I30 skipped. I28 and I31 are reduced forms, and I32 uses `Cow` instead of `&'static str`. I35 to I40, I42, I44 to I46, I48 and I49 (2026-11-15), I41, I43 and I47 skipped. I44 is applied to `with_authorization` only. I13 and I14 were applied first and then replaced by I23 (D62). I7 is the `jobs-env` crate, and I15.1 and I15.2 exist only there. In I16 the logger and the Azure pools are separate crates. I50 was written first as the jobs-only I16.1 and renumbered to match the other project. I50 and I52 (2026-11-16), I51 and I53 skipped, and the jobs-only I51.1. In I50 the subscriber image (`Dockerfile.subscriber`) was built and run by hand (2026-11-15). The API image (`Dockerfile`) was not built after the `COPY crates ./crates` change. I54 to I65 (2026-11-17): I55 to I57, I59 to I61, I63 to I65 applied, I54, I58 and I62 skipped. I65.1 exists only in jobs, and I65.2 and I65.3 were renumbered to I66 and I67 on 2026-11-19, when the other project got the same changes. Nothing of this pass was compiled where it was written, so `cargo verify` is the first check. I66 to I82 (2026-11-19): I66 to I72, I76, I77 and I81 applied, with the jobs-only I68.1, I73 to I75, I78, I79 and I82 skipped because jobs already has them, and I80 skipped. Nothing of the 2026-11-19 pass was compiled either. Both Docker images must be built by hand after I67.
- kafka-api-gateway-rust (2026-11-21): applied from the other projects: I1 to I9, I11, I13, I15, I23 to I28, I31 to I40, I42, I44, I47, I49, I51.1, I54 to I61, I63 to I65, I67, I72 and I74 to I79. Kafka only: I18.1, I21.1, I22.1, I39.1, I72.1, I73.1 and I74.1, with I20.1 skipped by decision. Skipped: I10, I12, I14, I15.1, I15.2, I16 to I22, I29, I29.1, I30, I30.1, I41, I43, I45, I46, I48, I50 to I53, I62, I65.1, I66, I68, I68.1, I69 to I71, I73 and I80 to I82. Nothing of this pass was compiled where it was written (no network, no registry). On the owner's machine `cargo verify` passed and the Docker image was built and ran correctly.

## Sources

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/): C-CALLER-CONTROL, C-GENERIC.
- [The Rust Performance Book, Heap Allocations](https://nnethercote.github.io/perf-book/heap-allocations.html)
- [The Rust Performance Book, Build Configuration](https://nnethercote.github.io/perf-book/build-configuration.html) and [Cargo Book, Profiles](https://doc.rust-lang.org/cargo/reference/profiles.html): I55.
- [MDN, Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control): I56. `immutable` is for urls that change with their content.
- [Cargo Book, Workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html): I64. `[workspace.dependencies]`.
- [serde, Container attributes](https://serde.rs/container-attrs.html): I65. `tag`, `rename_all` and `rename_all_fields`.
- [Docker, Optimize cache usage in builds](https://docs.docker.com/build/cache/optimize/): I67. Cache mounts for package managers.
- [Rust std, `TryFrom`](https://doc.rust-lang.org/std/convert/trait.TryFrom.html): I76. A conversion that can fail returns a `Result`.
- [axum, `Utf8Bytes`](https://docs.rs/axum/0.8.9/axum/extract/ws/struct.Utf8Bytes.html): I22.1. Cloning shares one buffer.
