package com.ekc395.digest.fetch;

public interface FeedAdapter {
    ParsedFeed parse(byte[] body) throws FeedParseException;
}
