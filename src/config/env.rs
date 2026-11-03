use std::sync::LazyLock;

use validator::Validate;

use crate::schemas::env::EnvSchema;

// Alias so the rest of the app refers to the schema as Env
pub type Env = EnvSchema;

// Global env, loaded and validated on first use, the process exits if it is invalid
static ENV: LazyLock<Env> = LazyLock::new(|| {
    Env::load().unwrap_or_else(|error| {
        eprintln!("[env] Environment validation failed: {error}");
        std::process::exit(1);
    })
});

// Returns the global env
pub fn env() -> &'static Env {
    &ENV
}

// Forces the env to load now, so a bad env stops the server at boot
pub fn init() {
    LazyLock::force(&ENV);
}

impl Env {
    // Reads all variables from the process env, trims the text values, then validates them
    pub fn load() -> Result<Self, String> {
        let mut env = envy::from_env::<Self>().map_err(|error| error.to_string())?;

        env.trim_fields();

        env.validate().map_err(|error| error.to_string())?;

        Ok(env)
    }

    // Removes leading and trailing spaces from every text value, like the old zod trim()
    fn trim_fields(&mut self) {
        for value in [
            &mut self.jwt_secret,
            &mut self.client_id,
            &mut self.client_secret,
            &mut self.app_name,
            &mut self.onprem_db_server,
            &mut self.onprem_db_database,
            &mut self.onprem_db_user_public,
            &mut self.onprem_db_password_public,
            &mut self.onprem_db_user_service,
            &mut self.onprem_db_password_service,
            &mut self.kafka_brokers,
            &mut self.kafka_client_id,
            &mut self.kafka_group_id,
        ] {
            *value = value.trim().to_string();
        }
    }
}
