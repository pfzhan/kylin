CREATE TABLE IF NOT EXISTS SPRING_SESSION (
    SESSION_ID VARCHAR(1024) NOT NULL,
    CREATION_TIME BIGINT NOT NULL,
    LAST_ACCESS_TIME BIGINT NOT NULL,
    MAX_INACTIVE_INTERVAL INT NOT NULL,
    PRINCIPAL_NAME VARCHAR(100),
    CONSTRAINT SPRING_SESSION_PK PRIMARY KEY (SESSION_ID)
);

CREATE INDEX SPRING_SESSION_IX1 ON SPRING_SESSION (LAST_ACCESS_TIME);
CREATE INDEX SPRING_SESSION_IX2 ON SPRING_SESSION (PRINCIPAL_NAME);