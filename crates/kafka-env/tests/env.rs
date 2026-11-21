use kafka_env::EnvSchema;

// A complete valid set of variables
fn valid_pairs() -> Vec<(String, String)> {
    [
        ("APP_ENV", "production"),
        ("APP_PORT", "8091"),
        ("JWT_SECRET", "jwt-secret"),
        ("CLIENT_ID", "caller-id"),
        ("CLIENT_SECRET", "caller-secret"),
        ("APP_NAME", "app"),
        ("ONPREM_DB_SERVER", "onprem"),
        ("ONPREM_DB_PORT", "5432"),
        ("ONPREM_DB_DATABASE", "onprem-db"),
        ("ONPREM_DB_USER_PUBLIC", "public-user"),
        ("ONPREM_DB_PASSWORD_PUBLIC", "public-password"),
        ("ONPREM_DB_USER_SERVICE", "service-user"),
        ("ONPREM_DB_PASSWORD_SERVICE", "service-password"),
        ("KAFKA_BROKERS", "127.0.0.1:9092"),
        ("KAFKA_CLIENT_ID", "kafka-client"),
        ("KAFKA_GROUP_ID", "kafka-group"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .collect()
}

// Returns the valid set with one variable replaced
fn with(name: &str, value: &str) -> Vec<(String, String)> {
    let mut pairs = valid_pairs();

    for pair in pairs.iter_mut() {
        if pair.0 == name {
            pair.1 = value.to_string();
        }
    }

    pairs
}

// Returns the valid set without one variable
fn without(name: &str) -> Vec<(String, String)> {
    valid_pairs()
        .into_iter()
        .filter(|pair| pair.0 != name)
        .collect()
}

// Extracts the error text, the schema has no Clone and a hand written Debug, so Result::unwrap_err is not used
fn error_of(pairs: Vec<(String, String)>) -> String {
    match EnvSchema::from_pairs(pairs) {
        Ok(_) => String::new(),
        Err(error) => error,
    }
}

// A complete valid set loads
#[test]
fn valid_env_loads() {
    let Ok(loaded) = EnvSchema::from_pairs(valid_pairs()) else {
        panic!("a valid env must load");
    };

    assert_eq!(loaded.app_env.as_str(), "production");
    assert_eq!(loaded.app_port, 8091);
    assert_eq!(loaded.kafka_group_id, "kafka-group");
}

// The port of the database falls back to 5432 when it is not set
#[test]
fn db_port_defaults_to_5432() {
    let Ok(loaded) = EnvSchema::from_pairs(without("ONPREM_DB_PORT")) else {
        panic!("a missing ONPREM_DB_PORT must use the default");
    };

    assert_eq!(loaded.onprem_db_port, 5432);
}

// A missing variable stops loading and names the variable
#[test]
fn missing_variable_names_it() {
    assert!(error_of(without("APP_PORT")).contains("APP_PORT"));
    assert!(error_of(without("JWT_SECRET")).contains("JWT_SECRET"));
    assert!(error_of(without("KAFKA_GROUP_ID")).contains("KAFKA_GROUP_ID"));
}

// APP_ENV accepts only development, preview and production
#[test]
fn app_env_rejects_other_values() {
    assert!(error_of(with("APP_ENV", "staging")).contains("APP_ENV"));
    assert!(EnvSchema::from_pairs(with("APP_ENV", "development")).is_ok());
    assert!(EnvSchema::from_pairs(with("APP_ENV", "preview")).is_ok());
    assert!(EnvSchema::from_pairs(with("APP_ENV", "production")).is_ok());
}

// Every APP_ENV value reads back with the same text
#[test]
fn app_env_text_matches_the_variable() {
    for value in ["development", "preview", "production"] {
        let Ok(loaded) = EnvSchema::from_pairs(with("APP_ENV", value)) else {
            panic!("{value} must load");
        };

        assert_eq!(loaded.app_env.as_str(), value);
    }
}

// Out of range numbers stop loading and name the variable
#[test]
fn out_of_range_numbers_name_the_variable() {
    assert!(error_of(with("APP_PORT", "0")).contains("APP_PORT"));
    assert!(error_of(with("ONPREM_DB_PORT", "0")).contains("ONPREM_DB_PORT"));
}

// A value that is not a number stops loading and names the variable
#[test]
fn non_numeric_port_names_the_variable() {
    assert!(error_of(with("APP_PORT", "abc")).contains("APP_PORT"));
    assert!(error_of(with("ONPREM_DB_PORT", "abc")).contains("ONPREM_DB_PORT"));
}

// Empty and too long values stop loading and name the variable
#[test]
fn length_rules_name_the_variable() {
    assert!(error_of(with("CLIENT_ID", "")).contains("CLIENT_ID"));
    assert!(error_of(with("KAFKA_BROKERS", "")).contains("KAFKA_BROKERS"));
    assert!(error_of(with("KAFKA_CLIENT_ID", &"k".repeat(101))).contains("KAFKA_CLIENT_ID"));
}

// A value of only spaces counts as empty because the text is trimmed before it is checked
#[test]
fn spaces_are_trimmed_before_the_check() {
    assert!(error_of(with("CLIENT_ID", "   ")).contains("CLIENT_ID"));

    let Ok(loaded) = EnvSchema::from_pairs(with("APP_NAME", "  app  ")) else {
        panic!("a value with spaces around it must load");
    };

    assert_eq!(loaded.app_name, "app");
}

// The error text never contains the secret value that failed
#[test]
fn error_text_does_not_contain_secret_values() {
    let secret = "s".repeat(51);
    let error = error_of(with("CLIENT_SECRET", &secret));

    assert!(error.contains("CLIENT_SECRET"));
    assert!(!error.contains(&secret));
}

// A value that cannot be parsed is not echoed in the error text
#[test]
fn unparsable_value_is_not_echoed() {
    let error = error_of(with("APP_PORT", "secret-port-text"));

    assert!(error.contains("APP_PORT"));
    assert!(!error.contains("secret-port-text"));
}

// Debug prints the safe fields and leaves out every secret
#[test]
fn debug_leaves_out_the_secrets() {
    let Ok(loaded) = EnvSchema::from_pairs(valid_pairs()) else {
        panic!("a valid env must load");
    };

    let text = format!("{loaded:?}");

    assert!(text.contains("app_port"));

    for secret in [
        "jwt-secret",
        "caller-id",
        "caller-secret",
        "public-user",
        "public-password",
        "service-user",
        "service-password",
    ] {
        assert!(!text.contains(secret), "Debug printed {secret}");
    }
}
