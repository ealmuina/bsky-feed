ALTER TABLE users
    ADD COLUMN refresh_frequency INT NOT NULL DEFAULT 30;