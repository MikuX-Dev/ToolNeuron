package com.dark.tool_neuron.plugins.services

import android.util.Log
import com.dark.tool_neuron.models.plugins.ScrapedContent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import java.io.IOException
import java.nio.charset.Charset
import java.util.Collections
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * Web scraping service with retry logic, UA rotation, cookie support,
 * multiple extraction strategies, readability scoring, and structured data parsing.
 */
class WebScrapingService {

    companion object {
        private const val TAG = "WebScrapingService"
        const val MAX_RETRIES = 3
        const val INITIAL_RETRY_DELAY = 1000L

        val CONTENT_SELECTORS = listOf(
            "article", "main", "[role=main]", ".post-content", ".entry-content",
            ".article-content", ".content", "#content", ".post", ".article"
        )

        val NOISE_SELECTORS = listOf(
            "script", "style", "nav", "header", "footer", "aside", ".sidebar",
            ".ads", ".advertisement", ".social-share", ".comments", "#comments",
            ".cookie-banner", ".popup", ".modal", "iframe[src*=ads]"
        )
    }

    // ── Thread-safe cookie jar ────────────────────────────────────────────────

    private val cookieJar = object : CookieJar {
        // synchronizedMap prevents concurrent modification from batchScrape
        private val store: MutableMap<String, MutableList<Cookie>> =
            Collections.synchronizedMap(mutableMapOf())

        override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
            store[url.host] = cookies.toMutableList()
        }

        override fun loadForRequest(url: HttpUrl): List<Cookie> =
            store[url.host] ?: emptyList()
    }

    // ── HTTP clients ──────────────────────────────────────────────────────────

    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .writeTimeout(15, TimeUnit.SECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(true)
        .cookieJar(cookieJar)
        .build()

    // Tight-timeout client for HEAD requests only
    private val headClient = OkHttpClient.Builder()
        .connectTimeout(8, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .build()

    // ── User-Agent pool ───────────────────────────────────────────────────────

    private val userAgents = listOf(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    )

    // ─────────────────────────────────────────────────────────────────────────
    // Public API — scrape
    // ─────────────────────────────────────────────────────────────────────────

    suspend fun scrape(
        url: String,
        selector: String? = null,
        maxLength: Int = 5000,
        useReadability: Boolean = true,
        extractStructuredData: Boolean = true,
    ): Result<ScrapedContent> = withContext(Dispatchers.IO) {
        if (!isValidUrl(url))
            return@withContext Result.failure(IllegalArgumentException("Invalid URL: $url"))

        var lastException: Exception? = null

        repeat(MAX_RETRIES) { attempt ->
            try {
                val t0 = System.currentTimeMillis()

                val request = Request.Builder()
                    .url(url)
                    .header("User-Agent", userAgents.random())
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("DNT", "1")
                    .header("Connection", "keep-alive")
                    .header("Upgrade-Insecure-Requests", "1")
                    .header("Sec-Fetch-Dest", "document")
                    .header("Sec-Fetch-Mode", "navigate")
                    .header("Sec-Fetch-Site", "none")
                    .header("Cache-Control", "max-age=0")
                    .build()

                client.newCall(request).execute().use { response ->
                    if (!response.isSuccessful)
                        throw IOException("HTTP ${response.code}: ${response.message}")

                    // Let OkHttp decode the body, but respect explicit charset in header
                    val contentType = response.header("Content-Type")
                    val charset = detectCharset(contentType)
                    val html = response.body.bytes().toString(charset)
                    val fetchTime = System.currentTimeMillis() - t0

                    val doc: Document = Jsoup.parse(html, url)

                    val content = when {
                        !selector.isNullOrBlank() -> extractBySelector(doc, selector)
                        useReadability -> extractWithReadability(doc)
                        else -> extractMainContent(doc)
                    }

                    return@withContext Result.success(
                        ScrapedContent(
                            url = url,
                            title = extractBestTitle(doc),
                            content = content.take(maxLength),
                            contentLength = content.length,
                            fetchTime = fetchTime,
                            metadata = extractEnhancedMetadata(
                                doc, selector, extractStructuredData, html.length
                            ),
                        )
                    )
                }
            } catch (e: Exception) {
                lastException = e
                Log.w(TAG, "scrape attempt ${attempt + 1} failed for $url: ${e.message}")
                if (attempt < MAX_RETRIES - 1)
                    delay(INITIAL_RETRY_DELAY * (1 shl attempt) + Random.nextLong(0, 500))
            }
        }

        Result.failure(lastException ?: IOException("Scraping failed after $MAX_RETRIES attempts"))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Content extraction strategies
    // ─────────────────────────────────────────────────────────────────────────

    private fun extractBySelector(doc: Document, selector: String): String {
        return try {
            val elements = doc.select(selector)
            if (elements.isEmpty()) return "No elements found for selector: $selector"
            buildString {
                elements.forEach { el ->
                    append(renderElementInOrder(el))
                    appendLine()
                }
            }.trim()
        } catch (e: Exception) {
            "Error with selector '$selector': ${e.message}"
        }
    }

    private fun extractMainContent(doc: Document): String {
        val clone = doc.clone()
        NOISE_SELECTORS.forEach { clone.select(it).remove() }

        val root = CONTENT_SELECTORS.firstNotNullOfOrNull { clone.selectFirst(it) }
            ?: clone.body()
            ?: return ""

        return renderElementInOrder(root)
    }

    /**
     * Readability-based extraction: scores candidate containers, picks the best one,
     * then renders it in document order.
     *
     * Key fix over original: candidates are evaluated bottom-up so a high-scoring
     * inner element wins over its lower-information-density ancestor.
     */
    private fun extractWithReadability(doc: Document): String {
        val clone = doc.clone()
        NOISE_SELECTORS.forEach { clone.select(it).remove() }

        data class Scored(val el: Element, val score: Int)

        val scored = clone.select("div, article, section, main").map { el ->
            val text = el.ownText() + el.select("p").joinToString(" ") { it.text() }
            val textLen = text.length
            val pCount = el.select("p").size
            val linkLen = el.select("a").sumOf { it.text().length }
            val linkDensity = if (textLen > 0) linkLen.toFloat() / textLen else 0f

            var score = (textLen / 100) + (pCount * 10) - (linkDensity * 50).toInt()

            val classes = el.classNames().joinToString(" ")
            if (classes.contains(Regex("content|article|post|body|story|text"))) score += 25
            if (classes.contains(Regex("comment|footer|sidebar|widget|menu|nav"))) score -= 25

            Scored(el, score)
        }

        // Among candidates with score > 0, prefer deepest (most specific) when scores are close
        val best = scored
            .filter { it.score > 0 }
            .maxWithOrNull(compareBy({ it.score }, { it.el.parents().size }))
            ?.el

        return if (best != null) renderElementInOrder(best)
        else extractMainContent(doc)
    }

    /**
     * Walk the element's child tree in document order and emit formatted text.
     *
     * Fix over original: instead of select()-ing all headings then all paragraphs
     * (which destroys order), we iterate children depth-first so the output
     * matches the page's reading sequence.
     */
    private fun renderElementInOrder(root: Element): String {
        val sb = StringBuilder()
        // Track visited elements so nested selects don't double-emit
        val visited = mutableSetOf<Element>()

        fun visit(el: Element) {
            if (!visited.add(el)) return
            when (el.tagName()) {
                "h1", "h2", "h3", "h4", "h5", "h6" -> {
                    val text = el.text().trim()
                    if (text.isNotBlank()) {
                        sb.appendLine()
                        sb.appendLine("## $text")
                    }
                    return // don't recurse into heading children separately
                }
                "p" -> {
                    val text = el.text().trim()
                    if (text.length > 10) {
                        sb.appendLine(text)
                        sb.appendLine()
                    }
                    return
                }
                "li" -> {
                    val text = el.text().trim()
                    if (text.isNotBlank()) sb.appendLine("• $text")
                    return
                }
                "blockquote" -> {
                    val text = el.text().trim()
                    if (text.isNotBlank()) {
                        sb.appendLine()
                        sb.appendLine("> $text")
                        sb.appendLine()
                    }
                    return
                }
                "pre", "code" -> {
                    val text = el.text().trim()
                    if (text.isNotBlank()) {
                        sb.appendLine()
                        sb.appendLine("```")
                        sb.appendLine(text)
                        sb.appendLine("```")
                        sb.appendLine()
                    }
                    return
                }
                "table" -> {
                    sb.appendLine()
                    el.select("tr").forEach { row ->
                        sb.appendLine(row.select("th, td").joinToString(" | ") { it.text().trim() })
                    }
                    sb.appendLine()
                    return
                }
                // For div/section/article/span etc., just recurse into children
                else -> el.children().forEach { child -> visit(child) }
            }
        }

        visit(root)
        return sb.toString().trim()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Title extraction
    // ─────────────────────────────────────────────────────────────────────────

    private fun extractBestTitle(doc: Document): String =
        listOfNotNull(
            doc.selectFirst("meta[property=og:title]")?.attr("content"),
            doc.selectFirst("meta[name=twitter:title]")?.attr("content"),
            doc.selectFirst("h1")?.text(),
            doc.title().takeIf { it.isNotBlank() },
        ).firstOrNull { it.isNotBlank() } ?: "Untitled"

    // ─────────────────────────────────────────────────────────────────────────
    // Charset detection — handles quoted values like charset="utf-8"
    // ─────────────────────────────────────────────────────────────────────────

    private fun detectCharset(contentType: String?): Charset {
        if (contentType == null) return Charsets.UTF_8
        return try {
            // Match both charset=utf-8 and charset="utf-8"
            val match = Regex("""charset=["']?([^"';\s]+)["']?""", RegexOption.IGNORE_CASE)
                .find(contentType)
            match?.groupValues?.getOrNull(1)?.let { Charset.forName(it) } ?: Charsets.UTF_8
        } catch (_: Exception) {
            Charsets.UTF_8
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Metadata
    // ─────────────────────────────────────────────────────────────────────────

    private fun extractEnhancedMetadata(
        doc: Document,
        selector: String?,
        includeStructuredData: Boolean,
        htmlSize: Int,
    ): Map<String, String> {
        val m = mutableMapOf<String, String>()

        fun meta(name: String, vararg selectors: String) {
            selectors.firstNotNullOfOrNull { sel ->
                doc.selectFirst(sel)?.attr("content")?.takeIf { it.isNotBlank() }
            }?.let { m[name] = it }
        }

        m["selector"] = selector ?: "auto"

        meta("description",
            "meta[name=description]", "meta[property=og:description]")
        meta("author",
            "meta[name=author]", "meta[property=article:author]")
        meta("keywords",       "meta[name=keywords]")
        meta("og:type",        "meta[property=og:type]")
        meta("og:image",       "meta[property=og:image]")
        meta("og:url",         "meta[property=og:url]")
        meta("og:site_name",   "meta[property=og:site_name]")
        meta("twitter:card",   "meta[name=twitter:card]")
        meta("twitter:site",   "meta[name=twitter:site]")
        meta("published_time", "meta[property=article:published_time]")
        meta("modified_time",  "meta[property=article:modified_time]")
        meta("section",        "meta[property=article:section]")
        meta("tags",           "meta[property=article:tag]")

        doc.selectFirst("link[rel=canonical]")?.attr("href")
            ?.takeIf { it.isNotBlank() }?.let { m["canonical_url"] = it }
        doc.selectFirst("link[rel=alternate]")?.attr("href")
            ?.takeIf { it.isNotBlank() }?.let { m["alternate_url"] = it }
        doc.selectFirst("link[rel*=icon]")?.attr("href")
            ?.takeIf { it.isNotBlank() }?.let { m["favicon"] = it }
        doc.selectFirst("link[type='application/rss+xml']")?.attr("href")
            ?.takeIf { it.isNotBlank() }?.let { m["rss_feed"] = it }
        doc.selectFirst("link[type='application/atom+xml']")?.attr("href")
            ?.takeIf { it.isNotBlank() }?.let { m["atom_feed"] = it }

        m["language"]          = doc.selectFirst("html")?.attr("lang") ?: "unknown"
        m["paragraph_count"]   = doc.select("p").size.toString()
        m["heading_count"]     = doc.select("h1,h2,h3,h4,h5,h6").size.toString()
        m["image_count"]       = doc.select("img").size.toString()
        m["link_count"]        = doc.select("a").size.toString()
        m["table_count"]       = doc.select("table").size.toString()
        m["list_count"]        = doc.select("ul,ol").size.toString()
        m["code_block_count"]  = doc.select("pre,code").size.toString()
        m["html_size_bytes"]   = htmlSize.toString()
        m["text_length"]       = doc.text().length.toString()

        if (includeStructuredData) {
            val sd = extractStructuredData(doc)
            if (sd.isNotBlank()) m["structured_data"] = sd
        }

        return m.filterValues { it.isNotBlank() }
    }

    private fun extractStructuredData(doc: Document): String = buildString {
        doc.select("script[type='application/ld+json']").forEach { script ->
            val json = script.data().trim()
            if (json.isNotEmpty()) {
                appendLine("JSON-LD:")
                appendLine(json)
                appendLine()
            }
        }
        doc.select("[itemscope]").take(5).forEach { item ->
            val itemType = item.attr("itemtype")
            if (itemType.isNotEmpty()) {
                appendLine("Microdata type: $itemType")
                item.select("[itemprop]").forEach { prop ->
                    val name  = prop.attr("itemprop")
                    val value = prop.attr("content").ifBlank { prop.text() }
                    if (name.isNotEmpty() && value.isNotEmpty()) appendLine("  $name: $value")
                }
                appendLine()
            }
        }
    }.trim()

    // ─────────────────────────────────────────────────────────────────────────
    // Public utility methods
    // ─────────────────────────────────────────────────────────────────────────

    suspend fun extractLinks(
        url: String,
        filterInternal: Boolean = false,
        filterExternal: Boolean = false,
    ): Result<List<String>> = withContext(Dispatchers.IO) {
        var lastEx: Exception? = null
        repeat(MAX_RETRIES) { attempt ->
            try {
                val req = Request.Builder().url(url)
                    .header("User-Agent", userAgents.random()).build()

                client.newCall(req).execute().use { response ->
                    if (!response.isSuccessful)
                        throw IOException("HTTP ${response.code}")

                    val doc = Jsoup.parse(response.body.string(), url)
                    val baseHost = runCatching { java.net.URI(url).host }.getOrNull()

                    val links = doc.select("a[href]")
                        .mapNotNull { a ->
                            val href = a.absUrl("href")
                            href.takeIf { it.startsWith("http://") || it.startsWith("https://") }
                        }
                        .filter { link ->
                            val host = runCatching { java.net.URI(link).host }.getOrNull()
                            when {
                                filterInternal && host == baseHost -> false
                                filterExternal && host != baseHost -> false
                                else -> true
                            }
                        }
                        .distinct()
                        .sorted()

                    return@withContext Result.success(links)
                }
            } catch (e: Exception) {
                lastEx = e
                if (attempt < MAX_RETRIES - 1)
                    delay(INITIAL_RETRY_DELAY * (1 shl attempt))
            }
        }
        Result.failure(lastEx ?: IOException("Link extraction failed"))
    }

    suspend fun extractImages(
        url: String,
        minWidth: Int = 0,
        minHeight: Int = 0,
    ): Result<List<String>> = withContext(Dispatchers.IO) {
        var lastEx: Exception? = null
        repeat(MAX_RETRIES) { attempt ->
            try {
                val req = Request.Builder().url(url)
                    .header("User-Agent", userAgents.random()).build()

                client.newCall(req).execute().use { response ->
                    if (!response.isSuccessful) throw IOException("HTTP ${response.code}")

                    val doc = Jsoup.parse(response.body.string(), url)
                    val images = mutableListOf<String>()

                    doc.select("img").forEach { img ->
                        val src = img.absUrl("src")
                        if (src.startsWith("http")) {
                            val w = img.attr("width").toIntOrNull() ?: 0
                            val h = img.attr("height").toIntOrNull() ?: 0
                            if (w >= minWidth && h >= minHeight) images.add(src)
                        }

                        // srcset — each entry is "<url> <descriptor>"
                        val srcset = img.attr("srcset")
                        if (srcset.isNotBlank()) {
                            srcset.split(",").forEach { entry ->
                                val candidate = entry.trim().split(Regex("\\s+")).firstOrNull()
                                    ?: return@forEach
                                // absUrl works for relative URLs; absolute ones pass through unchanged
                                val abs = if (candidate.startsWith("http")) candidate
                                          else img.absUrl(candidate)
                                if (abs.startsWith("http")) images.add(abs)
                            }
                        }
                    }

                    doc.selectFirst("meta[property=og:image]")?.attr("content")
                        ?.takeIf { it.startsWith("http") }?.let { images.add(it) }
                    doc.selectFirst("meta[name=twitter:image]")?.attr("content")
                        ?.takeIf { it.startsWith("http") }?.let { images.add(it) }
                    doc.select("link[rel*=icon]").forEach { link ->
                        val href = link.absUrl("href")
                        if (href.startsWith("http")) images.add(href)
                    }

                    return@withContext Result.success(images.distinct().sorted())
                }
            } catch (e: Exception) {
                lastEx = e
                if (attempt < MAX_RETRIES - 1)
                    delay(INITIAL_RETRY_DELAY * (1 shl attempt))
            }
        }
        Result.failure(lastEx ?: IOException("Image extraction failed"))
    }

    suspend fun extractResources(url: String): Result<Map<String, List<String>>> =
        withContext(Dispatchers.IO) {
            try {
                val req = Request.Builder().url(url)
                    .header("User-Agent", userAgents.random()).build()

                client.newCall(req).execute().use { response ->
                    if (!response.isSuccessful)
                        return@withContext Result.failure(IOException("HTTP ${response.code}"))

                    val doc = Jsoup.parse(response.body.string(), url)

                    Result.success(mapOf(
                        "css"    to doc.select("link[rel=stylesheet]")
                                      .mapNotNull { it.absUrl("href").takeIf { u -> u.startsWith("http") } }
                                      .distinct(),
                        "js"     to doc.select("script[src]")
                                      .mapNotNull { it.absUrl("src").takeIf { u -> u.startsWith("http") } }
                                      .distinct(),
                        "fonts"  to doc.select("link[rel=preload][as=font]")
                                      .mapNotNull { it.absUrl("href").takeIf { u -> u.startsWith("http") } }
                                      .distinct(),
                        "videos" to doc.select("video source[src]")
                                      .mapNotNull { it.absUrl("src").takeIf { u -> u.startsWith("http") } }
                                      .distinct(),
                        "audio"  to doc.select("audio source[src]")
                                      .mapNotNull { it.absUrl("src").takeIf { u -> u.startsWith("http") } }
                                      .distinct(),
                    ))
                }
            } catch (e: Exception) {
                Result.failure(e)
            }
        }

    suspend fun extractHeaders(url: String): Result<Map<String, String>> =
        withContext(Dispatchers.IO) {
            try {
                val req = Request.Builder().url(url)
                    .header("User-Agent", userAgents.random())
                    .head()
                    .build()

                // Use tight-timeout headClient, not the default 30s client
                headClient.newCall(req).execute().use { response ->
                    val headers = response.headers.names().associateWith { name ->
                        response.header(name) ?: ""
                    }
                    Result.success(headers)
                }
            } catch (e: Exception) {
                Result.failure(e)
            }
        }

    /**
     * Batch scrape — fixed delay logic to use index rather than object identity,
     * which broke when duplicate URLs were present in the list.
     */
    suspend fun batchScrape(
        urls: List<String>,
        maxLength: Int = 3000,
        delayBetweenRequests: Long = 1000L,
    ): Result<Map<String, ScrapedContent>> = withContext(Dispatchers.IO) {
        try {
            val results = mutableMapOf<String, ScrapedContent>()
            urls.forEachIndexed { index, url ->
                scrape(url, maxLength = maxLength).onSuccess { results[url] = it }
                if (index < urls.lastIndex) delay(delayBetweenRequests)
            }
            Result.success(results)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────────────────

    private fun isValidUrl(url: String): Boolean =
        url.lowercase().let { it.startsWith("http://") || it.startsWith("https://") }
}
