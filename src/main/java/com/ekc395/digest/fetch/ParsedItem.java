package com.ekc395.digest.fetch;

import java.time.Instant;

public record ParsedItem(
    String guid,
    String url,
    String title,
    String author,
    Instant publishedAt,
    String contentHtml
) {
}
