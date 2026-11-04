use std::env;
use std::process::{Command, exit};

use kafka_api_gateway::config::env::Env;

mod ws_test;

// Compose file, image and container names used by the docker tasks
const COMPOSE_FILE: &str = "docker/docker-compose.yml";
const IMAGE: &str = "kafka-api-gateway:latest";
const CONTAINER: &str = "kafka-api-gateway";

fn main() {
    let task = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo <css|dev|start|build-prod|hash|test-ws|docker-build|docker-up|docker-down|docker-start|docker-stop|docker-restart|docker-logs|docker-logs-clear|docker-rmi|docker-clean>");
        exit(1);
    });

    match task.as_str() {
        "css" => run_css(),
        "dev" => run_server("development", false),
        "start" => run_server("production", true),
        "build" => run_build(),
        "hash" => run_hash(env::args().nth(2)),
        "test-ws" => run_test_ws(env::args().nth(2), env::args().nth(3), env::args().nth(4)),
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

// Loads the env file for the profile into the process env, then validates it against the schema
fn load_env(profile: &str) -> Env {
    let env_file = format!(".env.{profile}");

    dotenvy::from_filename(&env_file)
        .unwrap_or_else(|_| panic!("Failed to load environment file: {env_file}"));

    Env::load().unwrap_or_else(|error| {
        eprintln!("[env] Validation failed for {env_file}: {error}");
        exit(1);
    })
}

// Compiles src/tailwind.css into public/global.css with the standalone Tailwind CLI
fn run_css() {
    let status = Command::new("tailwindcss")
        .args(["-i", "./src/tailwind.css", "-o", "./public/global.css"])
        .status()
        .expect("Failed to execute tailwindcss, is it installed and on PATH?");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

// Compiles Tailwind CSS, then runs the server, which inherits the variables loaded from the env file
fn run_server(profile: &str, release: bool) {
    let env = load_env(profile);

    run_css();

    println!(
        "-> Loaded .env.{profile} (Port: {}, Mode: {})",
        env.app_port,
        if release { "production" } else { "development" }
    );

    let mut cmd = Command::new("cargo");
    cmd.args(["run", "--package", "kafka-api-gateway"]);

    if release {
        cmd.arg("--release");
    }

    // On Unix this process is replaced by the gateway, so Ctrl+C reaches only the gateway and the prompt returns after its shutdown finished
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let error = cmd.exec();
        eprintln!("Failed to execute cargo run for kafka-api-gateway: {error}");
        exit(1);
    }

    #[cfg(not(unix))]
    {
        let status = cmd
            .status()
            .expect("Failed to execute cargo run for kafka-api-gateway");

        exit(status.code().unwrap_or(1));
    }
}

// Validates the production env, compiles Tailwind CSS, then builds in release mode
fn run_build() {
    load_env("production");

    run_css();

    let status = Command::new("cargo")
        .args(["build", "--release", "--package", "kafka-api-gateway"])
        .status()
        .expect("Failed to execute cargo build for kafka-api-gateway");

    exit(status.code().unwrap_or(1));
}

// Prints the bcrypt hash (cost 12) of the given value
fn run_hash(value: Option<String>) {
    let value = value.unwrap_or_else(|| {
        eprintln!("Usage: cargo hash 'value'");
        exit(1);
    });

    match bcrypt::hash(&value, 12) {
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

    let args = ws_test::Args {
        base: format!("localhost:{}", env.app_port),
        client_id: env.client_id.clone(),
        client_secret: env.client_secret.clone(),
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
    Command::new("docker")
        .args(args)
        .status()
        .expect("Failed to execute docker, is it installed and on PATH?")
        .success()
}

// Runs docker compose against the compose file and returns whether it succeeded
fn compose(args: &[&str]) -> bool {
    let mut full = vec!["compose", "-f", COMPOSE_FILE];
    full.extend_from_slice(args);

    docker(&full)
}

// Empties the container log file, which usually needs sudo on Linux
fn run_docker_logs_clear() -> ! {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.LogPath}}", CONTAINER])
        .output()
        .expect("Failed to execute docker inspect");

    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if !output.status.success() || path.is_empty() {
        eprintln!("Could not find the log file of the container {CONTAINER}, is it created?");
        exit(1);
    }

    let status = Command::new("sudo")
        .args(["truncate", "-s", "0", &path])
        .status()
        .expect("Failed to execute sudo truncate");

    finish(status.success())
}
