-- Runs once on first MySQL container start (mounted into /docker-entrypoint-initdb.d).
CREATE TABLE IF NOT EXISTS tasks (
  id          CHAR(36)     NOT NULL PRIMARY KEY,
  name        VARCHAR(128) NOT NULL,
  queue       VARCHAR(64)  NOT NULL,
  payload     JSON         NOT NULL,
  status      ENUM('queued','running','retrying','succeeded','dead','cancelled') NOT NULL DEFAULT 'queued',
  attempts    INT          NOT NULL DEFAULT 0,
  max_retries INT          NOT NULL DEFAULT 3,
  result      JSON         NULL,
  error       TEXT         NULL,
  worker_id   VARCHAR(128) NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at  DATETIME(6)  NULL,
  finished_at DATETIME(6)  NULL,
  KEY ix_tasks_status_created (status, created_at),
  KEY ix_tasks_queue_created (queue, created_at)
) ENGINE=InnoDB;
