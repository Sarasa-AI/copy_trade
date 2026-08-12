# Phase 0 / baseline safety checks (no DB required).

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_phase0_docs_exist():
    assert (ROOT / "PROJECT_UNDERSTANDING.md").is_file()
    assert (ROOT / "IMPLEMENTATION_ROADMAP.md").is_file()


def test_phase0_env_example_has_no_secrets():
    text = (ROOT / ".env.example").read_text()
    assert "CHANGE_ME" in text
    # Example file must stay placeholder-only
    assert "CHANGE_ME" in text
    for line in text.splitlines():
        if line.startswith("POSTGRES_PASSWORD=") or line.startswith("N8N_PASSWORD="):
            assert "CHANGE_ME" in line



def test_phase0_gitignore_includes_env():
    text = (ROOT / ".gitignore").read_text()
    assert ".env" in text.splitlines() or any(
        line.strip() == ".env" for line in text.splitlines()
    )


def test_phase1_alembic_layout_exists():
    assert (ROOT / "alembic.ini").is_file()
    assert (ROOT / "migrations" / "env.py").is_file()
    versions = list((ROOT / "migrations" / "versions").glob("*.py"))
    assert versions, "expected at least one Alembic revision"
    assert (ROOT / "requirements.txt").is_file()
    assert (ROOT / "db" / "README.md").is_file()


def test_schema_sql_declares_alembic_is_sot():
    text = (ROOT / "db" / "schema.sql").read_text()
    assert "Alembic" in text
    assert "wallet_id UUID" in text
    assert "REFERENCES wallets" in text


def test_compose_uses_migrate_not_schema_init():
    text = (ROOT / "docker-compose.yml").read_text()
    assert "migrate:" in text
    assert "alembic upgrade head" in text
    assert "01_schema.sql" not in text
