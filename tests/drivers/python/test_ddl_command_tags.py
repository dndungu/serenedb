"""Command tags of DDL that serenedb runs as pragmas or ATTACH match PostgreSQL."""

from __future__ import annotations

import os

import psycopg
from spec_loader import conn_kwargs


def test_ddl_command_tags():
    suffix = os.getpid()
    database = f"cmdtag_db_{suffix}"
    role = f"cmdtag_role_{suffix}"
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    try:
        with c.cursor() as cur:
            for statement, tag in (
                (f"CREATE DATABASE {database}", "CREATE DATABASE"),
                (f"DROP DATABASE {database}", "DROP DATABASE"),
                (f"DROP DATABASE IF EXISTS {database}", "DROP DATABASE"),
                (f"CREATE ROLE {role}", "CREATE ROLE"),
                (f"DROP ROLE {role}", "DROP ROLE"),
                (f"CREATE TABLE {role}_t (id INT)", "CREATE TABLE"),
                (f"CREATE TABLE {role}_log (id INT)", "CREATE TABLE"),
                (f"CREATE TRIGGER {role}_trg AFTER INSERT ON {role}_t FOR EACH ROW "
                 f"INSERT INTO {role}_log VALUES (NEW.id)", "CREATE TRIGGER"),
                (f"DROP TRIGGER {role}_trg ON {role}_t", "DROP TRIGGER"),
                (f"CREATE TEXT SEARCH DICTIONARY {role}_dict AS [keyword()]",
                 "CREATE TEXT SEARCH DICTIONARY"),
                (f"DROP TEXT SEARCH DICTIONARY {role}_dict", "DROP TEXT SEARCH DICTIONARY"),
                (f"DROP TABLE {role}_t", "DROP TABLE"),
                (f"DROP TABLE {role}_log", "DROP TABLE"),
            ):
                cur.execute(statement)
                assert cur.statusmessage == tag
                assert cur.description is None
    finally:
        c.close()
