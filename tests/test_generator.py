import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(tmp_path):
    subprocess.run(
        [sys.executable, str(ROOT / "data_generator" / "generate_data.py"), "--out", str(tmp_path),
         "--artists", "5", "--tracks", "10", "--users", "10", "--streams", "20"],
        check=True,
    )


def test_scripts_are_generated(tmp_path):
    run(tmp_path)
    initial = (tmp_path / "seed_initial.sql").read_text()
    incremental = (tmp_path / "seed_incremental.sql").read_text()
    for table in ("dim_artist", "dim_track", "dim_user", "dim_date", "fact_stream"):
        assert f"INSERT INTO dbo.{table}" in initial
    assert "UPDATE dbo.dim_user" in incremental
    assert "INSERT INTO dbo.fact_stream" in incremental


def test_output_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    run(a)
    run(b)
    assert (a / "seed_initial.sql").read_text() == (b / "seed_initial.sql").read_text()
