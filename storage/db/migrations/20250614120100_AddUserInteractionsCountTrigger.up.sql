CREATE OR REPLACE FUNCTION interactions_counter_update_to_user()
    RETURNS TRIGGER AS
$$
BEGIN
    CASE TG_OP
        WHEN 'INSERT' THEN UPDATE users AS u
                           SET interactions_count = COALESCE(interactions_count, 0) + 1
                           WHERE u.id = (SELECT author_id FROM posts WHERE id = NEW.post_id);
        WHEN 'DELETE' THEN UPDATE users AS u
                           SET interactions_count = GREATEST(0, COALESCE(interactions_count, 0) - 1)
                           WHERE u.id = (SELECT author_id FROM posts WHERE id = OLD.post_id);
        ELSE RAISE EXCEPTION 'Unexpected TG_OP: "%". Should not occur!', TG_OP;
        END CASE;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_interaction_counter
    AFTER INSERT OR DELETE
    ON interactions
    FOR EACH ROW
EXECUTE FUNCTION interactions_counter_update_to_user();
