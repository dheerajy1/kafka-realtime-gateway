import { About } from "@/components/About";
import openapi from "@elysiajs/openapi";
import { Elysia } from "elysia";
import React from "react";
import { renderToReadableStream } from "react-dom/server";

const router = new Elysia({ prefix: "/about" })
    .use(openapi({
        documentation: {
            tags: [
                { name: "portal-paths" },
                { name: "about" }
            ]
        }
    }))

const ABOUT_CONTENT = {
    title: "About | Kafka realtime gateway",
    heading: "About",
    description: `
    Kafka realtime gateway is a production-grade custom backend platform designed to prove
    clean contracts, strict validation, and zero drift between backend and frontend.

    It demonstrates how a modern Elysia + Bun backend can act as a single source
    of truth, exposing OpenAPI specifications that directly power frontend types,
    fetching, and caching strategies.
    `.trim(),
};

router.get("", async () => {
    const html = React.createElement(About, { title: ABOUT_CONTENT.title, heading: ABOUT_CONTENT.heading, description: ABOUT_CONTENT.description, });

    const stream = await renderToReadableStream(html);

    return new Response(stream, {
        headers: { "Content-Type": "text/html" }
    });
},
    {
        detail: {
            tags: ["portal-paths"]
        }
    }
);

export default router;
