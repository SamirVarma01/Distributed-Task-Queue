import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://dtq:dtq@localhost:3306/dtq")

# Queues the API accepts and workers consume, in priority order.
QUEUES = [q.strip() for q in os.getenv("QUEUES", "default").split(",") if q.strip()]

CONSUMER_GROUP = "dtq-workers"

# A message unacked for this long is assumed to belong to a dead worker and is reclaimed.
VISIBILITY_TIMEOUT_MS = int(os.getenv("VISIBILITY_TIMEOUT_MS", "30000"))
# Live workers refresh their in-flight message this often so it is never reclaimed.
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "10"))
RECLAIM_INTERVAL_SECONDS = float(os.getenv("RECLAIM_INTERVAL_SECONDS", "15"))

RETRY_BACKOFF_BASE_SECONDS = float(os.getenv("RETRY_BACKOFF_BASE_SECONDS", "2"))
RETRY_BACKOFF_MAX_SECONDS = float(os.getenv("RETRY_BACKOFF_MAX_SECONDS", "300"))
