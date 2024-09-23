ALTER TABLE users
    ADD engagement_factor FLOAT NULL;

CREATE INDEX idx_users_engagement_factor ON users (engagement_factor);
