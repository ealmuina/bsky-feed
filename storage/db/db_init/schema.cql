CREATE KEYSPACE bsky
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': '1'
        }
     AND durable_writes = true;

USE bsky;

CREATE TABLE IF NOT EXISTS follows
(
    uri         text,
    author_did  text,
    subject_did text,
    created_at  timestamp,
    PRIMARY KEY ( (uri), subject_did, created_at )
) WITH CLUSTERING ORDER BY (subject_did ASC, created_at DESC);

CREATE TABLE IF NOT EXISTS interactions
(
    uri        text,
    kind       text,
    author_did text,
    post_uri   text,
    created_at timestamp,
    PRIMARY KEY ( (uri), post_uri, created_at )
) WITH CLUSTERING ORDER BY (post_uri ASC, created_at DESC);

CREATE TABLE IF NOT EXISTS posts
(
    uri          text,
    author_did   text,
    reply_parent text,
    reply_root   text,
    created_at   timestamp,
    language     text,
    rank         double,
    PRIMARY KEY ( (uri), language, rank )
) WITH CLUSTERING ORDER BY (language ASC, rank DESC);

CREATE TABLE IF NOT EXISTS subscription_state
(
    service text PRIMARY KEY,
    cursor  bigint,
);

CREATE TABLE IF NOT EXISTS users
(
    did         text,
    handle      text,
    last_update timestamp,
    PRIMARY KEY ( (did), last_update )
);

CREATE TABLE IF NOT EXISTS users_counters
(
    did             text PRIMARY KEY,
    followers_count counter,
    follows_count   counter,
    posts_count     counter,
);
