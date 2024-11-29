CREATE OR REPLACE FUNCTION follow_counters_update_to_user()
    RETURNS TRIGGER AS
$$
BEGIN
    CASE TG_OP
        WHEN 'INSERT' THEN UPDATE users AS u -- Increment follows_count for the follower
                           SET follows_count = COALESCE(follows_count, 0) + 1
                           WHERE u.id = NEW.author_id;
                           UPDATE users AS u -- Increment followers_count for the followed
                           SET followers_count = COALESCE(followers_count, 0) + 1
                           WHERE u.id = NEW.subject_id;
        WHEN 'DELETE' THEN UPDATE users AS u -- Decrement follows_count for the follower
                           SET follows_count = GREATEST(0, COALESCE(follows_count, 0) - 1)
                           WHERE u.id = OLD.author_id;
                           UPDATE users AS u -- Decrement followers_count for the followed
                           SET followers_count = GREATEST(0, COALESCE(followers_count, 0) - 1)
                           WHERE u.id = OLD.subject_id;
        ELSE RAISE EXCEPTION 'Unexpected TG_OP: "%". Should not occur!', TG_OP;
        END CASE;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_follow_counter
    AFTER INSERT OR DELETE
    ON follows
    FOR EACH ROW
EXECUTE FUNCTION follow_counters_update_to_user();