use std::sync::LazyLock;

use validator::Validate;

use crate::schema::EnvSchema;

// Global env, loaded and validated on first use, the process exits if it is invalid
static ENV: LazyLock<EnvSchema> = LazyLock::new(|| {
    EnvSchema::load().unwrap_or_else(|error| {
        eprintln!("[env] Environment validation failed: {error}");
        std::process::exit(1);
    })
});

// Returns the global env
pub fn env() -> &'static EnvSchema {
    &ENV
}

// Forces the env to load now, so a bad env stops the server at boot
pub fn init() {
    LazyLock::force(&ENV);
}

// Prefix of the text envy gives for a variable that is not set
const MISSING_PREFIX: &str = "missing value for field ";

// Text envy puts before the variable name when a value cannot be parsed
const PROVIDED_BY: &str = " provided by ";

// Turns an envy error into a message that names the variable as written in the env file
// Envy gives the variable name in lowercase, the env file writes it in uppercase
fn describe_envy_error(error: &str) -> String {
    if let Some(field) = error.strip_prefix(MISSING_PREFIX) {
        return format!("{} is required", field.to_uppercase());
    }

    // APP_ENV is the only enum, envy gives no variable name for a wrong variant
    if error.starts_with("unknown variant") {
        return "APP_ENV must be development, preview or production".to_string();
    }

    // The parse error text holds the value, so only the variable name is kept
    if let Some((_, field)) = error.rsplit_once(PROVIDED_BY) {
        return format!("{} has an invalid value", field.to_uppercase());
    }

    error.to_string()
}

impl EnvSchema {
    // Reads all variables from the process env into the schema, then validates the values
    pub fn load() -> Result<Self, String> {
        Self::from_pairs(std::env::vars())
    }

    // Builds the schema from name and value pairs, used by load and by the tests
    // Text values are trimmed before they are validated, like the old zod trim()
    pub fn from_pairs<I>(pairs: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut env = envy::from_iter::<_, Self>(pairs)
            .map_err(|error| describe_envy_error(&error.to_string()))?;

        env.trim_fields();

        env.validate().map_err(|error| error.to_string())?;

        Ok(env)
    }

    // Removes leading and trailing spaces from every text value
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
            let trimmed = value.trim();

            // Copies only when there is something to cut off
            if trimmed.len() != value.len() {
                *value = trimmed.to_string();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A missing variable is named as written in the env file
    #[test]
    fn missing_variable_is_named_in_uppercase() {
        assert_eq!(
            describe_envy_error("missing value for field client_id"),
            "CLIENT_ID is required"
        );
    }

    // A wrong APP_ENV value gets the list of allowed values and never the value itself
    #[test]
    fn unknown_variant_names_the_allowed_values() {
        assert_eq!(
            describe_envy_error(
                "unknown variant `staging`, expected one of `development`, `preview`, `production`"
            ),
            "APP_ENV must be development, preview or production"
        );
    }

    // A value that cannot be parsed is reported by variable name only
    #[test]
    fn parse_error_keeps_only_the_variable_name() {
        let error = describe_envy_error(
            "invalid digit found in string while parsing value 'abc' provided by app_port",
        );

        assert_eq!(error, "APP_PORT has an invalid value");
        assert!(!error.contains("abc"));
    }

    // Any other text is passed on unchanged
    #[test]
    fn other_errors_pass_through() {
        assert_eq!(describe_envy_error("something else"), "something else");
    }
}
