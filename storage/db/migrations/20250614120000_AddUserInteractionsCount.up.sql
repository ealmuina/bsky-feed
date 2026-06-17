ALTER TABLE users
    ADD COLUMN interactions_count INT;

-- Backfill from existing interactions so the counter is authoritative
-- immediately after migration. The trigger created in the follow-up migration
-- only maintains interactions_count going forward; without this backfill every
-- existing user would read 0, making the engagement factor ~0 and starving the
-- top feeds until enough new interactions accrue.
--
-- interactions are pruned to a rolling 7-day window, so this counts the same
-- window the trigger will maintain. Users with no interactions stay NULL.
UPDATE users u
SET interactions_count = sub.c
FROM (
    SELECT p.author_id AS id, COUNT(*) AS c
    FROM interactions i
    JOIN posts p ON p.id = i.post_id
    GROUP BY p.author_id
) sub
WHERE u.id = sub.id;