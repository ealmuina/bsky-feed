CREATE TABLE subscription_state
(
    id      SERIAL PRIMARY KEY,
    service VARCHAR NOT NULL,
    cursor  BIGINT NOT NULL
);

CREATE UNIQUE INDEX "subscription_state_service" ON "subscription_state" ("service");