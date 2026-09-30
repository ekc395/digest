package com.ekc395.digest.feed;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import java.time.Instant;

@Entity
@Table(name = "feed")
public class Feed {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String url;
    private String siteUrl;
    private String title;
    private String kind;
    private boolean active;
    @Column(name = "fetch_interval_s")
    private int fetchIntervalS;
    private Instant nextFetchAt;
    private Instant lastSuccessAt;
    private int consecutiveFailures;
    private String etag;
    private String lastModified;
    private byte[] bodyHash;
    private Instant createdAt;

    protected Feed() {
        
    }

    public Long getId() {
        return this.id;
    }

    public String getUrl() {
        return this.url;
    }

    public String getSiteUrl() {
        return this.siteUrl;
    }

    public String getTitle() {
        return this.title;
    }

    public String getKind() {
        return this.kind;
    }

    public boolean isActive() {
        return this.active;
    }

    public int getFetchIntervalS() {
        return this.fetchIntervalS;
    }

    public Instant getNextFetchAt() {
        return this.nextFetchAt;
    }

    public void setNextFetchAt(Instant nextFetchAt) {
        this.nextFetchAt = nextFetchAt;
    }

    public Instant getLastSuccessAt() {
        return this.lastSuccessAt;
    }

    public void setLastSuccessAt(Instant lastSuccessAt) {
        this.lastSuccessAt = lastSuccessAt;
    }

    public int getConsecutiveFailures() {
        return this.consecutiveFailures;
    }

    public void setConsecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
    }

    public String getEtag() {
        return this.etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public String getLastModified() {
        return this.lastModified;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }

    public byte[] getBodyHash() {
        return this.bodyHash;
    }

    public void setBodyHash(byte[] bodyHash) {
        this.bodyHash = bodyHash;
    }

    public Instant getCreatedAt() {
        return this.createdAt;
    }
}
