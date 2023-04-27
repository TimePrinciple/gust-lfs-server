use std::env;

const KEY_PREFIX: &str = "LFS";

#[derive(Debug)]
pub struct Configuration {
    pub listen: String,
    pub host: String,
    pub ext_origin: String,
    pub meta_db: String,
    pub content_path: String,
    pub admin_user: String,
    pub admin_pass: String,
    pub cert: String,
    pub key: String,
    pub scheme: String,
    pub public: String,
    pub use_tus: String,
    pub tus_host: String,
}

impl Configuration {
    pub fn is_https(&self) -> bool {
        self.scheme.contains("https")
    }

    pub fn is_public(&self) -> bool {
        match self.public.as_str() {
            "1" | "true" | "TRUE" => true,
            _ => false,
        }
    }

    pub fn is_using_tus(&self) -> bool {
        match self.use_tus.as_str() {
            "1" | "true" | "TRUE" => true,
            _ => false,
        }
    }

    pub fn init() -> Configuration {
        let mut config = Configuration {
            listen: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "tcp://:8080".to_string();
                }
                let port = match env::var("PORT") {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if port != "" {
                    env = format!("tcp://:{}", port);
                }
                env
            },
            host: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "localhost:8080".to_string();
                }
                env
            },
            ext_origin: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                env
            },
            meta_db: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "lfs.db".to_string();
                }
                env
            },
            content_path: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "lfs-content".to_string();
                }
                // env
                "content-store".to_owned()
            },
            admin_user: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                env
            },
            admin_pass: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                env
            },
            cert: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                env
            },
            key: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                env
            },
            scheme: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "http".to_string();
                }
                env
            },
            public: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "public".to_string();
                }
                env
            },
            use_tus: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "false".to_string();
                }
                env
            },
            tus_host: {
                let env_var = format!("{}_{}", KEY_PREFIX, "");
                let mut env = match env::var(env_var) {
                    Ok(val) => val,
                    Err(_) => "".to_string(),
                };
                if env == "" {
                    env = "localhost:1080".to_string();
                }
                env
            },
        };
        let env = format!("{}://{}", config.scheme, config.host);

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        let config = Configuration::init();
        println!("{:?}", config);
    }
}
