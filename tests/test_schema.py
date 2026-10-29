"""sql/schema.sql creates the MySQL tables; the tests create theirs from the SQLAlchemy
models. Check the two agree, so a column added to one isn't missing from the other."""

import re
from pathlib import Path

import pytest

from dtq.agents.models import AgentRun, AgentStep
from dtq.db import Task

SCHEMA = (Path(__file__).parent.parent / "sql" / "schema.sql").read_text()


def schema_columns(table: str) -> dict[str, bool]:
    """{column: nullable} from the table's CREATE TABLE statement."""
    body = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\) ENGINE", SCHEMA, re.S).group(1)
    definitions: dict[str, str] = {}
    name = None
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        if line.startswith(("KEY", "PRIMARY KEY", "CONSTRAINT")):
            name = None
        elif re.match(r"[a-z_]+ ", line):
            name = line.split()[0]
            definitions[name] = line
        elif name:  # a definition continued from the previous line
            definitions[name] += " " + line
    return {n: "NOT NULL" not in d and "PRIMARY KEY" not in d for n, d in definitions.items()}


@pytest.mark.parametrize("model", [Task, AgentRun, AgentStep], ids=lambda m: m.__tablename__)
def test_model_matches_schema_sql(model):
    expected = schema_columns(model.__tablename__)
    actual = {c.name: c.nullable for c in model.__table__.columns}
    assert actual == expected
