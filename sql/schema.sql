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
  run_at      DATETIME(6)  NULL,
  enqueued_at DATETIME(6)  NULL,
  started_at  DATETIME(6)  NULL,
  finished_at DATETIME(6)  NULL,
  KEY ix_tasks_status_created (status, created_at),
  KEY ix_tasks_queue_created (queue, created_at)
) ENGINE=InnoDB;

-- Agent runtime (docs/agent-runtime-design.md, section 2). A run is one agent working
-- toward a goal; it advances in turns of one LLM call followed by that call's tool calls.
CREATE TABLE IF NOT EXISTS agent_runs (
  id               CHAR(36)    NOT NULL PRIMARY KEY,
  goal             TEXT        NOT NULL,
  status           ENUM('queued','running','awaiting_approval','succeeded','failed',
                        'cancelled','budget_exceeded') NOT NULL DEFAULT 'queued',
  model            JSON        NOT NULL,
  tools            JSON        NOT NULL,
  queue            VARCHAR(64) NOT NULL,
  step_max_retries INT         NOT NULL DEFAULT 3,
  -- Budgets. NULL means no limit; max_steps counts LLM calls.
  max_steps        INT         NOT NULL,
  max_tokens       BIGINT      NULL,
  max_cost_micros  BIGINT      NULL,
  max_wall_seconds INT         NULL,
  -- Usage totals, added to as each step is recorded. Cost is in millionths of a dollar.
  llm_calls        INT         NOT NULL DEFAULT 0,
  input_tokens     BIGINT      NOT NULL DEFAULT 0,
  output_tokens    BIGINT      NOT NULL DEFAULT 0,
  cost_micros      BIGINT      NOT NULL DEFAULT 0,
  -- Where the run is: the current turn's LLM step, its tool calls still without a
  -- result, and the next unused step number (step 0 is created with the run).
  turn             INT         NOT NULL DEFAULT 0,
  calls_remaining  INT         NOT NULL DEFAULT 0,
  next_step_no     INT         NOT NULL DEFAULT 1,
  -- Time spent awaiting approval, which doesn't count toward max_wall_seconds.
  paused_seconds   INT         NOT NULL DEFAULT 0,
  paused_at        DATETIME(6) NULL,
  result           JSON        NULL,
  error            TEXT        NULL,
  created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at       DATETIME(6) NULL,
  finished_at      DATETIME(6) NULL,
  KEY ix_agent_runs_status_created (status, created_at)
) ENGINE=InnoDB;

-- Append-only: a step's row is created when it is scheduled, and its outcome is
-- written once. A person's decision or an authorized retry is a new step.
CREATE TABLE IF NOT EXISTS agent_steps (
  run_id          CHAR(36)    NOT NULL,
  step_no         INT         NOT NULL,
  turn            INT         NOT NULL,
  kind            ENUM('llm','tool','approval') NOT NULL,
  status          ENUM('pending','started','waiting','succeeded','failed','unknown_outcome') NOT NULL,
  attempt         INT         NULL,
  call_id         VARCHAR(64) NULL,
  input           JSON        NOT NULL,
  output          JSON        NULL,
  input_tokens    INT         NULL,
  output_tokens   INT         NULL,
  cost_micros     BIGINT      NULL,
  idempotency_key VARCHAR(80) NULL,
  created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at      DATETIME(6) NULL,
  finished_at     DATETIME(6) NULL,
  PRIMARY KEY (run_id, step_no),
  CONSTRAINT fk_agent_steps_run FOREIGN KEY (run_id) REFERENCES agent_runs (id)
) ENGINE=InnoDB;
