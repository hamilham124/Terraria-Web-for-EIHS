import asyncio
import re
from urllib.parse import urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout, WSMsgType
from aiohttp import web
from multidict import CIMultiDict

UPSTREAM = "https://playterraria.lol"
PORT = 8000

HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

def clean_headers(source):
    blocked = set(HOP_HEADERS)

    connection = source.get("Connection", "")
    blocked.update(
        item.strip().lower()
        for item in connection.split(",")
        if item.strip()
    )

    result = CIMultiDict()

    for name, value in source.items():
        lower = name.lower()

        if lower in blocked:
            continue

        if lower == "host":
            continue

        if lower.startswith("sec-websocket-"):
            continue

        result.add(name, value)

    return result


def rewrite_location(location, request):
    if not location:
        return location

    parsed = urlsplit(location)

    if parsed.hostname == "playterraria.lol":
        return urlunsplit((
            "",
            "",
            parsed.path,
            parsed.query,
            parsed.fragment
        ))

    return location


def rewrite_cookie(cookie):
    # Remove the original domain so cookies can belong
    # to the Codespaces proxy hostname.
    return re.sub(
        r";\s*domain=[^;]+",
        "",
        cookie,
        flags=re.IGNORECASE
    )


async def websocket_proxy(request):
    session = request.app["session"]
    upstream_url = UPSTREAM + request.raw_path

    protocols = [
        item.strip()
        for item in request.headers.get(
            "Sec-WebSocket-Protocol", ""
        ).split(",")
        if item.strip()
    ]

    browser_ws = web.WebSocketResponse(
        protocols=protocols,
        heartbeat=30
    )

    try:
        async with session.ws_connect(
            upstream_url,
            headers=clean_headers(request.headers),
            protocols=protocols,
            heartbeat=30,
            autoping=True
        ) as upstream_ws:

            await browser_ws.prepare(request)

            async def browser_to_upstream():
                async for msg in browser_ws:
                    if msg.type == WSMsgType.TEXT:
                        await upstream_ws.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await upstream_ws.send_bytes(msg.data)
                    elif msg.type in (
                        WSMsgType.CLOSE,
                        WSMsgType.CLOSED,
                        WSMsgType.ERROR
                    ):
                        break

            async def upstream_to_browser():
                async for msg in upstream_ws:
                    if msg.type == WSMsgType.TEXT:
                        await browser_ws.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await browser_ws.send_bytes(msg.data)
                    elif msg.type in (
                        WSMsgType.CLOSE,
                        WSMsgType.CLOSED,
                        WSMsgType.ERROR
                    ):
                        break

            await asyncio.gather(
                browser_to_upstream(),
                upstream_to_browser(),
                return_exceptions=True
            )

    except Exception as error:
        print("WebSocket error:", error)
        if not browser_ws.prepared:
            return web.Response(
                status=502,
                text="Upstream WebSocket connection failed."
            )

    return browser_ws


async def proxy(request):
    if request.path == "/":
        raise web.HTTPFound("/game/")

    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await websocket_proxy(request)

    session = request.app["session"]

    upstream_url = UPSTREAM + request.raw_path

    try:
        async with session.request(
            method=request.method,
            url=upstream_url,
            headers=clean_headers(request.headers),
            data=request.content if request.can_read_body else None,
            allow_redirects=False
        ) as upstream:

            response_headers = CIMultiDict()

            for name, value in upstream.headers.items():
                lower = name.lower()

                if lower in HOP_HEADERS:
                    continue

                if lower == "set-cookie":
                    value = rewrite_cookie(value)

                if lower == "location":
                    value = rewrite_location(value, request)

                if lower == "access-control-allow-origin":
                    if value == UPSTREAM:
                        scheme = request.headers.get(
                            "X-Forwarded-Proto",
                            request.scheme
                        ).split(",")[0].strip()

                        value = f"{scheme}://{request.host}"

                response_headers.add(name, value)

            response = web.StreamResponse(
                status=upstream.status,
                reason=upstream.reason,
                headers=response_headers
            )

            await response.prepare(request)

            if (
                request.method != "HEAD"
                and upstream.status not in (204, 304)
            ):
                async for chunk in upstream.content.iter_chunked(
                    64 * 1024
                ):
                    await response.write(chunk)

            await response.write_eof()

            return response

    except Exception as error:
        print("Proxy error:", error)

        return web.Response(
            status=502,
            text=(
                "The Terraria upstream server could not be reached.\n"
                "Check the Codespaces terminal for details."
            )
        )


async def health(request):
    return web.json_response({
        "status": "online",
        "proxy": "Terraria Reverse Proxy",
        "upstream": UPSTREAM
    })


async def startup(app):
    timeout = ClientTimeout(
        total=None,
        connect=30,
        sock_read=None
    )

    app["session"] = ClientSession(
        timeout=timeout,
        auto_decompress=False
    )


async def cleanup(app):
    await app["session"].close()


app = web.Application(client_max_size=0)

app.router.add_get("/healthz", health)
app.router.add_route("*", "/{path:.*}", proxy)

app.on_startup.append(startup)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    print("Terraria Reverse Proxy starting...")
    print("Upstream:", UPSTREAM)
    print("Port:", PORT)

    web.run_app(
        app,
        host="0.0.0.0",
        port=PORT
    )
