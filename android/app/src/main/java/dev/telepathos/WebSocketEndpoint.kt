package dev.telepathos

import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import java.security.MessageDigest

/** Outcome of admitting a configured WebSocket endpoint before it can own local state. */
internal sealed interface WebSocketEndpointValidation {
    data class Valid(val canonicalUrl: String) : WebSocketEndpointValidation
    data class Invalid(val reason: String) : WebSocketEndpointValidation
}

/**
 * Canonical endpoint semantics are delegated to the same OkHttp URL parser
 * used to build the WebSocket request. The returned form lowercases scheme and
 * host, removes default ports, and always supplies a root slash.
 */
internal fun normalizeWebSocketEndpoint(raw: String): String {
    // HttpUrl intentionally accepts only HTTP(S). Request.Builder.url(String)
    // applies this exact ws/wss -> http/https mapping before handing the URL to
    // HttpUrl, so do the same here and restore the WebSocket scheme afterward.
    // This keeps durable identity and the actual OkHttp request on one parser.
    val websocketScheme = when {
        raw.startsWith("ws://", ignoreCase = true) -> "ws"
        raw.startsWith("wss://", ignoreCase = true) -> "wss"
        else -> throw IllegalArgumentException("WebSocket URL must use ws:// or wss://")
    }
    val httpScheme = if (websocketScheme == "ws") "http" else "https"
    val httpUrl = (httpScheme + raw.substring(websocketScheme.length)).toHttpUrlOrNull()
        ?: throw IllegalArgumentException("invalid WebSocket URL")
    val authorityStart = websocketScheme.length + 3
    val authorityEnd = raw.indexOfAny(charArrayOf('/', '?', '#'), authorityStart)
        .let { if (it == -1) raw.length else it }
    require('@' !in raw.substring(authorityStart, authorityEnd)) {
        "WebSocket URL must not contain userinfo"
    }
    require(httpUrl.username.isEmpty() && httpUrl.password.isEmpty()) {
        "WebSocket URL must not contain userinfo"
    }
    require(httpUrl.query == null && httpUrl.fragment == null) {
        "WebSocket URL must not contain a query or fragment"
    }
    val canonicalHttp = httpUrl.toString()
    return websocketScheme + canonicalHttp.substring(httpScheme.length)
}

/**
 * Validate untrusted configuration without letting a malformed persisted value
 * escape into service lifecycle or durable-receipt ownership code.
 */
internal fun validateWebSocketEndpoint(raw: String): WebSocketEndpointValidation =
    try {
        WebSocketEndpointValidation.Valid(normalizeWebSocketEndpoint(raw))
    } catch (error: IllegalArgumentException) {
        WebSocketEndpointValidation.Invalid(error.message ?: "invalid WebSocket URL")
    }

internal fun equivalentWebSocketEndpoint(left: String, right: String): Boolean =
    runCatching { normalizeWebSocketEndpoint(left) == normalizeWebSocketEndpoint(right) }
        .getOrDefault(false)

internal fun sha256Hex(value: String): String =
    MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8))
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
