# The schema is copied in at build time rather than bind-mounted, so the stack works
# from any directory (Docker Desktop on macOS only mounts shared paths such as /Users).
FROM mysql:8.4
COPY schema.sql /docker-entrypoint-initdb.d/001-schema.sql
