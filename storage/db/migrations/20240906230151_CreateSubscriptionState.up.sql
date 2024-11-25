CREATE TABLE subscription_state
(
    id      SERIAL PRIMARY KEY,
    service TEXT NOT NULL,
    cursor  TEXT NOT NULL
);

CREATE UNIQUE INDEX "subscription_state_service" ON "subscription_state" ("service");