DROP INDEX idx_users_engagement_factor;

ALTER TABLE users
    DROP COLUMN engagement_factor;
