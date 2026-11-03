use dioxus::prelude::*;

use crate::components::layout::Layout;

// Navigation links shown at the top of the Home page as (href, label)
const NAV_LINKS: [(&str, &str); 7] = [
    ("/", "Home"),
    ("/about", "About"),
    ("/openapi/json", "OpenAPI spec"),
    ("/openapi", "Scalar UI"),
    ("/healthz", "Health"),
    ("/healthz/app", "Health app"),
    ("/healthz/db", "Health db"),
];

// Home page rendered inside the shared Layout
#[component]
pub fn Home() -> Element {
    rsx! {
        Layout { title: "Kafka realtime gateway, Axum + Dioxus SSR".to_string(),
            div { class: "max-w-(--width-container) w-full mx-auto p-4 md:p-8 leading-relaxed",
                nav { class: "flex flex-wrap gap-4 mb-4 px-4 md:px-0",
                    for (href , label) in NAV_LINKS {
                        a {
                            key: "{href}",
                            href: "{href}",
                            class: "font-medium no-underline transition-colors duration-200 text-link hover:text-text-primary hover:underline",
                            "{label}"
                        }
                    }
                }
                section { class: "flex flex-col gap-4 bg-surface border border-border rounded-lg p-4 md:p-8 mt-8",
                    h1 { class: "text-3xl font-bold text-text-primary",
                        "Welcome to Kafka realtime gateway, Axum + Dioxus SSR 🚀"
                    }
                    p { class: "text-base leading-relaxed text-text",
                        "Complete foundational structure, type safety layer, deployment readiness, and basic login authentication for the Kafka realtime gateway API backend, built with Rust, Axum and Dioxus SSR."
                    }
                    img {
                        src: "/logo.png",
                        alt: "Logo",
                        class: "w-(--width-logo) h-auto border border-border-thick rounded-md",
                    }
                }
            }
        }
    }
}
