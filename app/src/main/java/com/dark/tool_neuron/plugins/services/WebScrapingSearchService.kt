package com.dark.tool_neuron.plugins.services

import android.util.Log
import com.dark.tool_neuron.models.plugins.DuckDuckGoSearchResponse
import com.dark.tool_neuron.models.plugins.SearchResult
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jsoup.Jsoup
import java.io.IOException
import java.net.URLEncoder
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * Multi-engine web search via HTML scraping.
 *
 * Engine priority (each is tried in order until results are found):
 *   1. DuckDuckGo  — POST html.duckduckgo.com/html/  (most scraping-tolerant)
 *   2. Bing        — GET bing.com/search              (stable selectors, low bot detection)
 *   3. Brave       — GET search.brave.com/search      (independent index, relaxed scraping)
 *
 * All engines return [DuckDuckGoSearchResponse] for drop-in compatibility.
 */
class WebScrapingSearchService {

    companion object {
        private const val TAG = "WebScrapingSearch"
        private const val MAX_RETRIES = 2
        private const val RETRY_BASE_MS = 1000L
        private const val MIN_QUERY_LENGTH = 1
        private const val MAX_QUERY_LENGTH = 500
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP client — shared across all engines
    // ─────────────────────────────────────────────────────────────────────────

    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(true)
        .build()

    // ─────────────────────────────────────────────────────────────────────────
    // User-Agent pools — kept realistic and varied
    // ─────────────────────────────────────────────────────────────────────────

    private val desktopUAs = listOf(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    )

    private val mobileUAs = listOf(
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    )

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    suspend fun search(
        query: String,
        maxResults: Int = 5,
        safeSearch: Boolean = true,
        @Suppress("UNUSED_PARAMETER") region: String? = null,
        @Suppress("UNUSED_PARAMETER") timeRange: String? = null,
    ): Result<DuckDuckGoSearchResponse> = withContext(Dispatchers.IO) {

        val q = sanitizeQuery(query)
        if (q.length < MIN_QUERY_LENGTH)
            return@withContext Result.failure(IllegalArgumentException("Query too short"))
        if (q.length > MAX_QUERY_LENGTH)
            return@withContext Result.failure(IllegalArgumentException("Query too long"))

        val cap = maxResults.coerceIn(1, 10)

        // ── Engine 1: DuckDuckGo ──────────────────────────────────────────
        scrapeDuckDuckGo(q, cap, safeSearch).onSuccess { r ->
            if (r.results.isNotEmpty()) {
                Log.d(TAG, "DDG: ${r.results.size} results")
                return@withContext Result.success(r)
            }
        }.onFailure { Log.w(TAG, "DDG failed: ${it.message}") }

        delay(300 + Random.nextLong(200))

        // ── Engine 2: Bing ────────────────────────────────────────────────
        scrapeBing(q, cap, safeSearch).onSuccess { r ->
            if (r.results.isNotEmpty()) {
                Log.d(TAG, "Bing: ${r.results.size} results")
                return@withContext Result.success(r)
            }
        }.onFailure { Log.w(TAG, "Bing failed: ${it.message}") }

        delay(300 + Random.nextLong(200))

        // ── Engine 3: Brave ───────────────────────────────────────────────
        scrapeBrave(q, cap, safeSearch).onSuccess { r ->
            if (r.results.isNotEmpty()) {
                Log.d(TAG, "Brave: ${r.results.size} results")
                return@withContext Result.success(r)
            }
        }.onFailure { Log.w(TAG, "Brave failed: ${it.message}") }

        Result.failure(IOException("All search engines failed for: $q"))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Engine 1 — DuckDuckGo HTML (POST, most reliable)
    // ─────────────────────────────────────────────────────────────────────────

    private suspend fun scrapeDuckDuckGo(
        query: String,
        maxResults: Int,
        safeSearch: Boolean,
    ): Result<DuckDuckGoSearchResponse> = withContext(Dispatchers.IO) {
        var lastEx: Exception? = null
        repeat(MAX_RETRIES) { attempt ->
            try {
                val t0 = System.currentTimeMillis()

                // DDG works much better via POST to its HTML endpoint
                val body = FormBody.Builder()
                    .add("q", query)
                    .add("kl", "us-en")          // region
                    .add("kp", if (safeSearch) "1" else "-2") // safe search
                    .add("kaf", "1")              // no ads
                    .build()

                val req = Request.Builder()
                    .url("https://html.duckduckgo.com/html/")
                    .post(body)
                    .header("User-Agent", desktopUAs.random())
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Origin", "https://duckduckgo.com")
                    .header("Referer", "https://duckduckgo.com/")
                    .header("DNT", "1")
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (!resp.isSuccessful) throw IOException("DDG HTTP ${resp.code}")
                    val html = resp.body.string()
                    val results = parseDuckDuckGoHtml(html, maxResults)
                    return@withContext Result.success(
                        DuckDuckGoSearchResponse(
                            query = query,
                            results = results,
                            totalResults = results.size,
                            searchTime = System.currentTimeMillis() - t0,
                        )
                    )
                }
            } catch (e: Exception) {
                lastEx = e
                if (attempt < MAX_RETRIES - 1)
                    delay(RETRY_BASE_MS * (1 shl attempt) + Random.nextLong(500))
            }
        }
        Result.failure(lastEx ?: IOException("DDG unknown failure"))
    }

    private fun parseDuckDuckGoHtml(html: String, max: Int): List<SearchResult> {
        val results = mutableListOf<SearchResult>()
        val seen = mutableSetOf<String>()
        try {
            val doc = Jsoup.parse(html)

            // DDG HTML layout: each result is <div class="result results_links ...">
            // Title  : <a class="result__a" href="...">
            // Snippet: <a class="result__snippet">  or  <div class="result__body">
            val items = doc.select("div.result.results_links, div.result.results_links_deep")
            for (item in items) {
                if (results.size >= max) break

                val anchor = item.selectFirst("a.result__a") ?: continue
                val title = anchor.text().trim()
                if (title.isBlank()) continue

                // DDG wraps the real URL in a redirect — the data-href or href attr
                // has the actual URL directly (unlike Google)
                val href = anchor.attr("href").trim()
                val url = when {
                    href.startsWith("http") -> href
                    href.startsWith("//") -> "https:$href"
                    else -> continue   // relative or empty — skip
                }
                if (url in seen) continue
                seen.add(url)

                val snippet = item.selectFirst("a.result__snippet")?.text()?.trim()
                    ?: item.selectFirst("div.result__body")?.text()?.trim()
                    ?: ""

                results.add(SearchResult(title, snippet, url, results.size + 1))
            }

            // Fallback: older DDG layout uses <div class="links_main links_deep result__body">
            if (results.isEmpty()) {
                val fallbackItems = doc.select("div.links_main")
                for (item in fallbackItems) {
                    if (results.size >= max) break
                    val a = item.selectFirst("a[href]") ?: continue
                    val title = a.text().trim().ifBlank { continue }
                    val href = a.attr("href").takeIf { it.startsWith("http") } ?: continue
                    if (href in seen) continue
                    seen.add(href)
                    val snippet = item.selectFirst("a.result__snippet, span.result__snippet")
                        ?.text()?.trim() ?: ""
                    results.add(SearchResult(title, snippet, href, results.size + 1))
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "DDG parse error: ${e.message}")
        }
        return results
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Engine 2 — Bing (GET, stable DOM structure)
    // ─────────────────────────────────────────────────────────────────────────

    private suspend fun scrapeBing(
        query: String,
        maxResults: Int,
        safeSearch: Boolean,
    ): Result<DuckDuckGoSearchResponse> = withContext(Dispatchers.IO) {
        var lastEx: Exception? = null
        repeat(MAX_RETRIES) { attempt ->
            try {
                val t0 = System.currentTimeMillis()
                val encoded = URLEncoder.encode(query, "UTF-8")
                val safe = if (safeSearch) "Moderate" else "Off"
                val url = "https://www.bing.com/search?q=$encoded&count=${maxResults + 3}&setlang=en&safeSearch=$safe"

                val req = Request.Builder()
                    .url(url)
                    .header("User-Agent", desktopUAs.random())
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("DNT", "1")
                    .header("Connection", "keep-alive")
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (resp.code == 429) throw RateLimitException("Bing rate limited")
                    if (!resp.isSuccessful) throw IOException("Bing HTTP ${resp.code}")
                    val html = resp.body.string()
                    val results = parseBingHtml(html, maxResults)
                    return@withContext Result.success(
                        DuckDuckGoSearchResponse(
                            query = query,
                            results = results,
                            totalResults = results.size,
                            searchTime = System.currentTimeMillis() - t0,
                        )
                    )
                }
            } catch (e: Exception) {
                lastEx = e
                if (attempt < MAX_RETRIES - 1)
                    delay(RETRY_BASE_MS * (1 shl attempt) + Random.nextLong(500))
            }
        }
        Result.failure(lastEx ?: IOException("Bing unknown failure"))
    }

    private fun parseBingHtml(html: String, max: Int): List<SearchResult> {
        val results = mutableListOf<SearchResult>()
        val seen = mutableSetOf<String>()
        try {
            val doc = Jsoup.parse(html)

            // Bing result structure (stable since ~2020):
            //   <li class="b_algo">
            //     <h2><a href="https://...">Title</a></h2>
            //     <div class="b_caption"><p>Snippet text</p></div>
            val items = doc.select("li.b_algo")
            for (item in items) {
                if (results.size >= max) break

                val h2 = item.selectFirst("h2") ?: continue
                val a = h2.selectFirst("a[href]") ?: continue
                val title = a.text().trim().ifBlank { continue }

                val href = a.attr("href").trim()
                if (!href.startsWith("http")) continue
                if (href in seen) continue
                seen.add(href)

                val snippet = item.selectFirst("div.b_caption p, div.b_algoSlug, p.b_lineclamp2")
                    ?.text()?.trim() ?: ""

                results.add(SearchResult(title, snippet, href, results.size + 1))
            }
        } catch (e: Exception) {
            Log.w(TAG, "Bing parse error: ${e.message}")
        }
        return results
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Engine 3 — Brave Search (GET, independent index, lax bot detection)
    // ─────────────────────────────────────────────────────────────────────────

    private suspend fun scrapeBrave(
        query: String,
        maxResults: Int,
        safeSearch: Boolean,
    ): Result<DuckDuckGoSearchResponse> = withContext(Dispatchers.IO) {
        var lastEx: Exception? = null
        repeat(MAX_RETRIES) { attempt ->
            try {
                val t0 = System.currentTimeMillis()
                val encoded = URLEncoder.encode(query, "UTF-8")
                val safe = if (safeSearch) "moderate" else "off"
                val url = "https://search.brave.com/search?q=$encoded&count=${maxResults + 3}&lang=en&safesearch=$safe&source=web"

                val req = Request.Builder()
                    .url(url)
                    .header("User-Agent", mobileUAs.random()) // mobile UA avoids some JS walls on Brave
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("DNT", "1")
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (resp.code == 429) throw RateLimitException("Brave rate limited")
                    if (!resp.isSuccessful) throw IOException("Brave HTTP ${resp.code}")
                    val html = resp.body.string()
                    val results = parseBraveHtml(html, maxResults)
                    return@withContext Result.success(
                        DuckDuckGoSearchResponse(
                            query = query,
                            results = results,
                            totalResults = results.size,
                            searchTime = System.currentTimeMillis() - t0,
                        )
                    )
                }
            } catch (e: Exception) {
                lastEx = e
                if (attempt < MAX_RETRIES - 1)
                    delay(RETRY_BASE_MS * (1 shl attempt) + Random.nextLong(500))
            }
        }
        Result.failure(lastEx ?: IOException("Brave unknown failure"))
    }

    private fun parseBraveHtml(html: String, max: Int): List<SearchResult> {
        val results = mutableListOf<SearchResult>()
        val seen = mutableSetOf<String>()
        try {
            val doc = Jsoup.parse(html)

            // Brave result structure:
            //   <div class="snippet fdb"> or <div class="snippet">
            //     <a class="heading-serpresult" href="https://...">
            //       <span class="title">Title</span>
            //     </a>
            //     <div class="snippet-description">Snippet</div>
            val items = doc.select("div.snippet[data-type=web], div.snippet.fdb")
            for (item in items) {
                if (results.size >= max) break

                val a = item.selectFirst("a[href^=https]") ?: continue
                val title = (item.selectFirst("span.title, .title") ?: a).text().trim()
                if (title.isBlank()) continue

                val href = a.attr("href").trim()
                if (href in seen) continue
                seen.add(href)

                val snippet = item.selectFirst("div.snippet-description, p.snippet-description")
                    ?.text()?.trim() ?: ""

                results.add(SearchResult(title, snippet, href, results.size + 1))
            }

            // Fallback: generic heading + URL pattern in Brave's SSR output
            if (results.isEmpty()) {
                doc.select("a[href^=https]").forEach { a ->
                    if (results.size >= max) return@forEach
                    val href = a.attr("href")
                    if (href.contains("brave.com") || href in seen) return@forEach
                    val title = a.text().trim()
                    if (title.length < 10) return@forEach
                    seen.add(href)
                    results.add(SearchResult(title, "", href, results.size + 1))
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Brave parse error: ${e.message}")
        }
        return results
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utilities
    // ─────────────────────────────────────────────────────────────────────────

    private fun sanitizeQuery(query: String) = query
        .trim()
        .replace(Regex("\\s+"), " ")
        .replace(Regex("[\\x00-\\x1F\\x7F]"), "")

    private class RateLimitException(msg: String) : IOException(msg)
}
