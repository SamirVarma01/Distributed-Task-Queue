"""The tables sql/schema.sql creates in MySQL, compared with the SQLAlchemy models the
code uses. tests/test_schema.py compares the two by reading the SQL file; this reads the
tables MySQL actually created."""

import pytest
from sqlalchemy import DateTime, Enum, String, inspect
from sqlalchemy.dialects import mysql

from dtq.agents.demo import SinkEntry
from dtq.agents.models import AgentRun, AgentStep
from dtq.db import Task, engine

pytestmark = pytest.mark.integration
MODELS = [Task, AgentRun, AgentStep, SinkEntry]


@pytest.fixture(scope="module")
def tables():
    insp = inspect(engine)
    return {m.__tablename__: ({c["name"]: c for c in insp.get_columns(m.__tablename__)},
                              insp.get_pk_constraint(m.__tablename__)["constrained_columns"]) for m in MODELS}


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__tablename__)
def test_mysql_table_matches_the_model(model, tables):
    columns, primary_key = tables[model.__tablename__]

    assert {name: c["nullable"] for name, c in columns.items()} == \
           {c.name: c.nullable for c in model.__table__.columns}
    assert primary_key == [c.name for c in model.__table__.primary_key]
    for col in model.__table__.columns:
        actual = columns[col.name]["type"]
        if isinstance(col.type, Enum):
            assert list(actual.enums) == list(col.type.enums), col.name
        elif isinstance(col.type, String) and col.type.length:
            assert actual.length == col.type.length, col.name


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__tablename__)
def test_timestamps_keep_microseconds(model, tables):
    """The benchmarks compute sub-millisecond latencies from these columns; a plain
    DATETIME would round them to whole seconds."""
    columns, _ = tables[model.__tablename__]
    for col in model.__table__.columns:
        if isinstance(col.type, DateTime):
            actual = columns[col.name]["type"]
            assert isinstance(actual, mysql.DATETIME) and actual.fsp == 6, col.name
