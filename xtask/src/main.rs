use std::env;
use std::ffi::OsStr;
use std::process::{Command, ExitStatus, exit};

use kafka_env::EnvSchema as Env;
use quiet::Show;

mod quiet;
mod ws_test;

// Compose file, image and container names used by the docker tasks
const COMPOSE_FILE: &str = "docker/docker-compose.yml";
const IMAGE: &str = "kafka-api-gateway:latest";
const CONTAINER: &str = "kafka-api-gateway";

// The one package the build and run tasks work on
const PACKAGE: &str = "kafka-api-gateway";

// bcrypt cost of `cargo hash`, the same as BCRYPT_COST in the gateway (src/auth/password.rs)
const BCRYPT_COST: u32 = 12;

fn main() {
    let task = env::args().nth(1).unwrap_or_else(|| {
        eprintln!(
            "Usage: cargo <css|dev|start|preview|build-prod|build-preview|hash|test-ws|verify|docker-build|docker-up|docker-down|docker-start|docker-stop|docker-restart|docker-logs|docker-logs-clear|docker-rmi|docker-clean>"
        );
        exit(1);
    });

    match task.as_str() {
        "css" => run_css(),
        "dev" => run_server("development", false),
        "start" => run_server("production", true),
        "preview" => run_server("preview", true),
        "build" => run_build("production"),
        "build-preview" => run_build("preview"),
        "hash" => run_hash(env::args().nth(2)),
        "test-ws" => run_test_ws(env::args().nth(2), env::args().nth(3), env::args().nth(4)),
        "verify" => run_verify(),
        "docker-build" => {
            run_css();
            finish(compose(&["build"]))
        }
        "docker-up" => finish(compose(&["up", "-d"])),
        "docker-down" => finish(compose(&["down"])),
        "docker-start" => finish(compose(&["start"])),
        "docker-stop" => finish(compose(&["stop"])),
        "docker-restart" => finish(compose(&["restart"])),
        "docker-logs" => finish(compose(&["logs", "-f"])),
        "docker-logs-clear" => run_docker_logs_clear(),
        "docker-rmi" => finish(docker(&["rmi", IMAGE])),
        "docker-clean" => {
            let down = compose(&["down"]);
            let rmi = docker(&["rmi", IMAGE]);
            finish(down && rmi)
        }
        _ => {
            eprintln!("Unknown task: {task}");
            exit(1);
        }
    }
}

// Exits with the exit code of a finished command, or 1 when a signal ended it
fn exit_with(status: ExitStatus) -> ! {
    exit(status.code().unwrap_or(1))
}

// Runs a command and returns its exit status, or exits with a clear message when it cannot start
fn status_or_exit(command: &mut Command, failure: &str) -> ExitStatus {
    command.status().unwrap_or_else(|error| {
        eprintln!("{failure}: {error}");
        exit(1);
    })
}

// Loads the env file for the profile into the process env, then validates it against the schema
fn load_env(profile: &str) -> Env {
    let env_file = format!(".env.{profile}");

    dotenvy::from_filename(&env_file).unwrap_or_else(|error| {
        eprintln!("Failed to load environment file {env_file}: {error}");
        exit(1);
    });

    Env::load().unwrap_or_else(|error| {
        eprintln!("[env] Validation failed for {env_file}: {error}");
        exit(1);
    })
}

// Compiles src/tailwind.css into public/global.css with the standalone Tailwind CLI
fn run_css() {
    let status = status_or_exit(
        Command::new("tailwindcss").args(["-i", "./src/tailwind.css", "-o", "./public/global.css"]),
        "Failed to execute tailwindcss, is it installed and on PATH?",
    );

    if !status.success() {
        exit_with(status);
    }
}

// Cargo arguments for the gateway package, with the release flag added only when asked
fn package_args(command: &str, release: bool) -> impl Iterator<Item = &str> {
    [command, "--package", PACKAGE]
        .into_iter()
        .chain(release.then_some("--release"))
}

// Runs one cargo command quietly, exits with code 1 when it fails
fn run_step<I, S>(args: I, show: Show)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new("cargo");
    cmd.args(args);

    match quiet::run(cmd, show) {
        Ok(true) => {}
        Ok(false) => exit(1),
        Err(error) => {
            eprintln!("Failed to run cargo: {error}");
            exit(1);
        }
    }
}

// Compiles Tailwind CSS, builds the gateway quietly, then runs it with the variables loaded from the env file
fn run_server(profile: &str, release: bool) -> ! {
    let env = load_env(profile);

    run_css();

    println!(
        "-> Loaded .env.{profile} (Port: {}, Mode: {})",
        env.app_port,
        env.app_env.as_str()
    );

    run_step(package_args("build", release), Show::All);

    let mut cmd = Command::new("cargo");
    cmd.args(package_args("run", release));
    cmd.arg("--quiet");
    quiet::clean_cargo_env(&mut cmd);

    // On Unix this process is replaced by the gateway, so Ctrl+C reaches only the gateway and the prompt returns after its shutdown finished
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let error = cmd.exec();
        eprintln!("Failed to execute cargo run for {PACKAGE}: {error}");
        exit(1);
    }

    #[cfg(not(unix))]
    {
        let status = status_or_exit(&mut cmd, "Failed to execute cargo run");

        exit_with(status)
    }
}

// Validates the env file of the profile, compiles Tailwind CSS, then builds in release mode
fn run_build(profile: &str) {
    load_env(profile);

    run_css();

    println!("-> Validated .env.{profile}");

    run_step(package_args("build", true), Show::All);
}

// Runs fmt, check, clippy and test in order and stops at the first failure
fn run_verify() {
    let steps: [(&str, &[&str], Show); 4] = [
        ("cargo fmt", &["fmt"], Show::All),
        (
            "cargo check",
            &["check", "--workspace", "--all-targets"],
            Show::All,
        ),
        (
            "cargo clippy",
            &[
                "clippy",
                "--workspace",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
            Show::All,
        ),
        ("cargo test", &["test", "--workspace"], Show::TestSummary),
    ];

    for (label, args, show) in steps {
        println!("-> {label}");

        run_step(args, show);
    }

    println!("-> verify passed");
}

// Prints the bcrypt hash of the given value
fn run_hash(value: Option<String>) {
    let value = value.unwrap_or_else(|| {
        eprintln!("Usage: cargo hash 'value'");
        exit(1);
    });

    match bcrypt::hash(&value, BCRYPT_COST) {
        Ok(hash) => println!("{hash}"),
        Err(error) => {
            eprintln!("Failed to hash the value: {error}");
            exit(1);
        }
    }
}

// Runs the WebSocket scenario against the gateway that is already running with the development env
fn run_test_ws(api_key: Option<String>, api_secret: Option<String>, mode: Option<String>) {
    let (Some(api_key), Some(api_secret)) = (api_key, api_secret) else {
        eprintln!("Usage: cargo test-ws 'api-key-id' 'api-key-secret' [hold]");
        exit(1);
    };

    let env = load_env("development");

    // The env is not used after this, so its texts move into the arguments
    let args = ws_test::Args {
        base: format!("localhost:{}", env.app_port),
        client_id: env.client_id,
        client_secret: env.client_secret,
        api_key,
        api_secret,
    };

    // The hold mode waits for the gateway to shut down instead of running the scenario
    if mode.as_deref() == Some("hold") {
        exit(if ws_test::hold(&args) { 0 } else { 1 });
    }

    exit(if ws_test::run(&args) { 0 } else { 1 });
}

// Exits with 0 when the command succeeded and 1 otherwise
fn finish(ok: bool) -> ! {
    exit(if ok { 0 } else { 1 })
}

// Runs a docker command and returns whether it succeeded
fn docker(args: &[&str]) -> bool {
    status_or_exit(
        Command::new("docker").args(args),
        "Failed to execute docker, is it installed and on PATH?",
    )
    .success()
}

// Runs docker compose against the compose file and returns whether it succeeded
fn compose(args: &[&str]) -> bool {
    status_or_exit(
        Command::new("docker")
            .args(["compose", "-f", COMPOSE_FILE])
            .args(args),
        "Failed to execute docker compose, is Docker installed?",
    )
    .success()
}

// Empties the container log file, which usually needs sudo on Linux
fn run_docker_logs_clear() -> ! {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.LogPath}}", CONTAINER])
        .output()
        .unwrap_or_else(|error| {
            eprintln!("Failed to execute docker inspect: {error}");
            exit(1);
        });

    let path = String::from_utf8_lossy(&output.stdout);
    let path = path.trim();

    if !output.status.success() || path.is_empty() {
        eprintln!("Could not find the log file of the container {CONTAINER}, is it created?");
        exit(1);
    }

    let status = status_or_exit(
        Command::new("sudo").args(["truncate", "-s", "0", path]),
        "Failed to execute sudo truncate",
    );

    exit_with(status)
}
