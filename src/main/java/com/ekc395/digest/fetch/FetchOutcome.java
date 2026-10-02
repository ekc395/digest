package com.ekc395.digest.fetch;

public sealed interface FetchOutcome {
    record Ok(ParsedFeed feed, int httpStatus, int bytes) implements FetchOutcome {}
    record NotModified() implements FetchOutcome {}
    record HttpError(int status) implements FetchOutcome {}
    record Timeout() implements FetchOutcome {}
    record ParseError(String errorClass, String detail) implements FetchOutcome {}
    record Empty(int httpStatus, int bytes) implements FetchOutcome {}
}
