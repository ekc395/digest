package com.ekc395.digest.fetch;

import org.springframework.stereotype.Component;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import com.ekc395.digest.feed.Feed;

@Component
public class FeedFetcher {
    private static final String USER_AGENT = "Digest/0.1 (ekc395@gmail.com)";
    private static final int MAX_BYTES = 5 * 1024 * 1024;

    private final HttpClient client;

    public FeedFetcher() {
        this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    public FetchOutcome fetch(Feed feed) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(feed.getUrl()))
            .timeout(Duration.ofSeconds(30))
            .header("User-Agent", USER_AGENT)
            .GET()
            .build();
        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            int status = response.statusCode();
            try (InputStream body = response.body()) {
                if (status == 304) {
                    return new FetchOutcome.NotModified();
                }
                if (status < 200 || status >= 300) {
                    return new FetchOutcome.HttpError(status);
                }
                byte[] bytes = body.readNBytes(MAX_BYTES + 1);
                if (bytes.length > MAX_BYTES) {
                    return new FetchOutcome.ParseError("ResponseTooLarge", "body exceeds " + MAX_BYTES + " bytes");
                }
                if (bytes.length == 0) {
                    return new FetchOutcome.Empty(status, 0);
                }
                throw new UnsupportedOperationException("needs FeedAdapter (step 7)");
            }
        } catch (HttpTimeoutException e) {
            return new FetchOutcome.Timeout();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
