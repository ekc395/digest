-- Feed URLs, not site URLs: url is the XML endpoint the fetcher GETs,
-- site_url is the human page the digest email links to.
INSERT INTO feed (url, site_url, title, kind, fetch_interval_s) VALUES
    -- arXiv announces once per day in a single batch, so hourly polling would
    -- be 23 wasted fetches. Every item also shares one pubDate (see locked
    -- decision 2) -- this feed is only orderable by first_seen_at.
    ('https://rss.arxiv.org/rss/cs.AI',
     'https://arxiv.org/list/cs.AI/recent',
     'arXiv cs.AI',
     'arxiv',
     86400),

    -- Rate-limits hard and requires a descriptive User-Agent; the generic
    -- adapter parses it until the Phase 2 Reddit adapter lands.
    ('https://www.reddit.com/r/investing/.rss',
     'https://www.reddit.com/r/investing/',
     'r/investing',
     'reddit',
     3600),

    ('https://www.cnbc.com/id/100003114/device/rss/rss.html',
     'https://www.cnbc.com/',
     'CNBC Top News',
     'rss',
     3600);
