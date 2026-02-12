from pathlib import Path

import duckdb
import polars as pl

cwd = Path().cwd()
db_path = cwd / "wbn.duckdb"
assert db_path.exists()

con = duckdb.connect(db_path, read_only=True)

vars = con.table("uebersicht")
votes = con.table("bundeswahl2025").filter("regexp_matches(Ortsbezirk, '^[0-9]{2}')")
