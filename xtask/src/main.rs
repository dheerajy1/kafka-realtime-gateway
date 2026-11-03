use std::env;
use std::process::{Command, exit};

use kafka_api_gateway::config::env::Env;

fn main() {
    let task = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo <css|dev|start|build-prod|hash>");
        exit(1);
    });

    match task.as_str() {
        "css" => run_css(),
        "dev" => run_server("development", false),
        "start" => run_server("production", true),
        "build" => run_build(),
        "hash" => run_hash(env::args().nth(2)),
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

    let status = cmd
        .status()
        .expect("Failed to execute cargo run for kafka-api-gateway");

    exit(status.code().unwrap_or(1));
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
