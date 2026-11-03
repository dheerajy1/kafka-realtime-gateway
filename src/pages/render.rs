use axum::{
    http::{HeaderMap, header},
    response::Html,
};
use dioxus::prelude::*;

// Renders a component without props into a full html document
pub fn render(app: fn() -> Element) -> Html<String> {
    let mut dom = VirtualDom::new(app);
    dom.rebuild_in_place();
    wrap(dioxus_ssr::render(&dom))
}

// Renders a component with props into a full html document
pub fn render_with_props<P: Clone + 'static>(app: fn(P) -> Element, props: P) -> Html<String> {
    let mut dom = VirtualDom::new_with_props(app, props);
    dom.rebuild_in_place();
    wrap(dioxus_ssr::render(&dom))
}

// Adds the doctype and html tag around the rendered head and body
fn wrap(body: String) -> Html<String> {
    Html(format!("<!DOCTYPE html><html lang=\"en\">{body}</html>"))
}

// True when the request Accept header asks for html, as a browser does
pub fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}
