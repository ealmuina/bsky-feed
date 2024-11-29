CREATE OR REPLACE FUNCTION posts_counter_update_to_user()
    RETURNS TRIGGER AS
$$
BEGIN
    CASE TG_OP
        WHEN 'INSERT' THEN UPDATE users AS u
                           SET posts_count = COALESCE(posts_count, 0) + 1
                           WHERE u.id = NEW.author_id;
        WHEN 'DELETE' THEN UPDATE users AS u
                           SET posts_count = GREATEST(0, COALESCE(posts_count, 0) - 1)
                           WHERE u.id = OLD.author_id;
        ELSE RAISE EXCEPTION 'Unexpected TG_OP: "%". Should not occur!', TG_OP;
        END CASE;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_post_counter
    AFTER INSERT OR DELETE
    ON posts
    FOR EACH ROW
EXECUTE FUNCTION posts_counter_update_to_user();