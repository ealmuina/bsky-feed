ALTER TABLE users
    ADD COLUMN interactions_count INT;

-- interactions_count is maintained going forward by the trigger created in the
-- follow-up migration (an AFTER INSERT/DELETE trigger on interactions, mirroring
-- the existing posts_count trigger).
--
-- Do NOT backfill here. A single-statement backfill over the (large) interactions
-- table needs temp space for the GROUP BY that exceeded available disk on the
-- production host and aborted the migration, leaving the DB in a dirty state.
-- Migrations must stay cheap and schema-only. The 7-day interactions window
-- self-corrects within 7 days of running; if accurate counters are wanted sooner,
-- run a batched backfill as a separate one-off operation, not in a migration.