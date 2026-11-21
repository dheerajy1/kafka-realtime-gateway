use axum::{
    http::{HeaderMap, header},
    response::Html,
};
use dioxus::prelude::*;
use dioxus_ssr::Renderer;

// Opening of every page, written first so the page body is rendered straight after it
const DOCUMENT_START: &str = "<!DOCTYPE html><html lang=\"en\">";

// Closing of every page, pushed after the body
const DOCUMENT_END: &str = "</html>";

// Renders a component without props into a full html document
pub fn render(app: fn() -> Element) -> Html<String> {
    let mut dom = VirtualDom::new(app);
    dom.rebuild_in_place();

    render_document(&dom)
}

// Renders a component with props into a full html document
pub fn render_with_props<P: Clone + 'static>(app: fn(P) -> Element, props: P) -> Html<String> {
    let mut dom = VirtualDom::new_with_props(app, props);
    dom.rebuild_in_place();

    render_document(&dom)
}

// Writes the doctype, the page and the closing tag into one String, so the page is never copied
fn render_document(dom: &VirtualDom) -> Html<String> {
    let mut html = String::from(DOCUMENT_START);

    // Writing into a String cannot fail, so an error here is a bug in dioxus-ssr
    Renderer::new()
        .render_to(&mut html, dom)
        .expect("rendering into a String cannot fail");

    html.push_str(DOCUMENT_END);

    Html(html)
}

// True when the request Accept header asks for html, as a browser does
pub fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}
