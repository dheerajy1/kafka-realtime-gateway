// The env schema and the global env live in the kafka-env crate, so xtask can read the same env without building the server
// This module keeps the paths the rest of the app uses
pub use kafka_env::{EnvSchema as Env, env, init};
