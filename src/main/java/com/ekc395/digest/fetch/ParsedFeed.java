package com.ekc395.digest.fetch;

import java.util.List;

public record ParsedFeed(
    String title,
    String siteUrl,
    List<ParsedItem> items
) {
    public ParsedFeed {
        items = List.copyOf(items);
    }
}
