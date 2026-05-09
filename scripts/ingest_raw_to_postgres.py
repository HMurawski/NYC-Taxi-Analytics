"""
Raw ingestion for NYC TLC Yellow Taxi data into Postgres.

Design goals:
- Keep raw data as close to the source as possible (no filtering/cleaning here).
- Stream / chunk-load Parquet so we do not hold the full dataset in memory.
- Be idempotent for local development by using a stable raw_trip_id
- and insert-if-not-exists logic for immutable source files.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow as pa
import pyarrow.parquet as pq


RAW_DIR_DEFAULT = Path("data") / "raw"
TRIPS_GLOB_DEFAULT = "yellow_tripdata_*.parquet"
ZONES_FILE_DEFAULT = "taxi_zone_lookup.csv"


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: Optional[str] = None


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def load_pg_config_from_env() -> PgConfig:
    """
    Use standard libpq-style environment variables.
    This keeps the script compatible with common Postgres tooling.
    """

    sslmode = os.getenv("PGSSLMODE")
    return PgConfig(
        host=_env("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=_env("PGDATABASE"),
        user=_env("PGUSER"),
        password=_env("PGPASSWORD"),
        sslmode=sslmode,
    )


def connect(cfg: PgConfig):
    conn_kwargs = {
        "host": cfg.host,
        "port": cfg.port,
        "dbname": cfg.dbname,
        "user": cfg.user,
        "password": cfg.password,
    }
    # sslmode is optional; only apply it when set explicitly.
    if cfg.sslmode:
        conn_kwargs["sslmode"] = cfg.sslmode
    return psycopg2.connect(**conn_kwargs)


_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9]+")
_CAMEL_1_RE = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_2_RE = re.compile(r"([a-z0-9])([A-Z])")


def to_snake_case(name: str) -> str:
    """
    Normalize names to snake_case.

    TLC columns are usually already close to snake_case, but we normalize anyway
    to keep raw schema consistent and prevent surprises later in dbt.
    """

    name = name.strip()
    if not name:
        return name

    # Handle common camelCase / PascalCase patterns first.
    name = _CAMEL_1_RE.sub(r"\1_\2", name)
    name = _CAMEL_2_RE.sub(r"\1_\2", name)

    # Replace any remaining punctuation/whitespace with underscores.
    name = _NON_ALNUM_RE.sub("_", name)
    name = name.strip("_").lower()

    # Postgres identifiers must not start with a digit.
    if name and name[0].isdigit():
        name = f"col_{name}"
    return name


def dedupe_column_names(names: list[str]) -> list[str]:
    """
    Ensure unique column names after normalization.

    This is defensive: if two source columns normalize to the same snake_case name,
    we add suffixes to avoid silently dropping a column.
    """

    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        base = n
        if base not in seen:
            seen[base] = 1
            out.append(base)
            continue
        seen[base] += 1
        out.append(f"{base}_{seen[base]}")
    return out


def arrow_type_to_pg(t: pa.DataType) -> str:
    """
    Map Arrow data types to reasonable Postgres column types.

    We keep this mapping intentionally simple; raw ingestion isn't the place
    to enforce business-domain typing beyond safe fidelity.
    """

    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_int8(t) or pa.types.is_int16(t):
        return "smallint"
    if pa.types.is_int32(t):
        return "integer"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_uint8(t) or pa.types.is_uint16(t):
        return "integer"
    if pa.types.is_uint32(t) or pa.types.is_uint64(t):
        return "bigint"
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "real"
    if pa.types.is_float64(t):
        return "double precision"
    if pa.types.is_decimal(t):
        # Keep exact precision/scale when present.
        return f"numeric({t.precision}, {t.scale})"
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return "date"
    if pa.types.is_timestamp(t):
        # TLC timestamps are typically local time without tz; use timestamp.
        return "timestamp"
    if pa.types.is_time32(t) or pa.types.is_time64(t):
        return "time"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "bytea"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "text"
    if pa.types.is_dictionary(t):
        # Dictionary-encoded strings are common in Parquet; store as text.
        return "text"
    if pa.types.is_struct(t) or pa.types.is_list(t) or pa.types.is_large_list(t):
        # Uncommon for TLC data; store as jsonb if it appears.
        return "jsonb"
    return "text"


def execute_sql(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def rebuild_raw_schema(conn) -> None:
    """
    Local-dev convenience: drop and recreate the entire raw schema.

    This is intentionally destructive and should not be used in production runs.
    """

    execute_sql(conn, "DROP SCHEMA IF EXISTS raw CASCADE;")
    execute_sql(conn, "CREATE SCHEMA raw;")


def ensure_raw_schema(conn) -> None:
    execute_sql(conn, "CREATE SCHEMA IF NOT EXISTS raw;")


def get_trip_files(raw_dir: Path, trips_glob: str) -> list[Path]:
    files = sorted(raw_dir.glob(trips_glob))
    if not files:
        raise FileNotFoundError(f"No trip parquet files found at {raw_dir / trips_glob}")
    return files


def parse_year_month_from_filename(path: Path) -> tuple[int, int]:
    """
    Expect filenames like yellow_tripdata_2025-01.parquet.
    """

    m = re.search(r"(\d{4})-(\d{2})", path.name)
    if not m:
        raise ValueError(f"Could not parse YYYY-MM from filename: {path.name}")
    return int(m.group(1)), int(m.group(2))


def stable_raw_trip_id(source_file: str, source_row_number: int) -> str:
    """
    Stable id across re-runs for idempotent ingestion.

    We intentionally do NOT hash the full row payload; the goal is “one row per
    source record” keyed by its file+position, which is deterministic.
    """

    s = f"{source_file}:{source_row_number}".encode("utf-8")
    return hashlib.md5(s).hexdigest()


def ensure_raw_tables(conn, sample_trip_file: Path, zones_csv: Path) -> None:
    """
    Create raw tables if they do not exist.

    For trips, we infer columns from a sample Parquet file.
    For zones, we infer columns from the CSV header.
    """

    pf = pq.ParquetFile(sample_trip_file)
    arrow_schema = pf.schema_arrow
    src_cols = [to_snake_case(n) for n in arrow_schema.names]
    src_cols = dedupe_column_names(src_cols)

    # Build trips table columns.
    trip_col_defs: list[str] = []
    for field, col_name in zip(arrow_schema, src_cols, strict=True):
        trip_col_defs.append(f"{col_name} {arrow_type_to_pg(field.type)}")

    # Add ingestion metadata columns.
    trip_col_defs.extend(
        [
            "raw_trip_id text NOT NULL",
            "source_file text NOT NULL",
            "source_year integer NOT NULL",
            "source_month integer NOT NULL",
            "source_row_number bigint NOT NULL",
            "loaded_at timestamptz NOT NULL DEFAULT now()",
        ]
    )

    ddl_trips = f"""
    CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips (
        {",\n        ".join(trip_col_defs)},
        CONSTRAINT yellow_taxi_trips_pk PRIMARY KEY (raw_trip_id),
        CONSTRAINT yellow_taxi_trips_source_row_uk UNIQUE (source_file, source_row_number)
    );
    """

    # Zone lookup: read header only, normalize names, dedupe.
    zone_df = pd.read_csv(zones_csv, nrows=0)
    zone_cols = dedupe_column_names([to_snake_case(c) for c in zone_df.columns.tolist()])
    # Standard TLC zone lookup includes LocationID/Borough/Zone/service_zone.
    # We keep all as text except location_id.
    zone_col_defs: list[str] = []
    for c in zone_cols:
        if c == "location_id":
            zone_col_defs.append("location_id integer NOT NULL")
        else:
            zone_col_defs.append(f"{c} text")

    ddl_zones = f"""
    CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
        {",\n        ".join(zone_col_defs)},
        CONSTRAINT taxi_zone_lookup_location_id_pk PRIMARY KEY (location_id)
    );
    """

    execute_sql(conn, ddl_trips)
    execute_sql(conn, ddl_zones)


def iter_parquet_batches(
    parquet_file: Path,
    batch_size: int,
) -> Iterator[pa.RecordBatch]:
    pf = pq.ParquetFile(parquet_file)
    yield from pf.iter_batches(batch_size=batch_size)


def _df_to_rows(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """
    Convert DataFrame to a list of tuples for execute_values.

    Pandas uses NaN/NaT for missing values; psycopg2 expects Python None.
    """

    # Convert to object dtype and replace NaN/NaT with None efficiently.
    sub = df[columns].astype(object)
    sub = sub.where(pd.notnull(sub), None)
    return list(map(tuple, sub.to_numpy()))


def load_trips(
    conn,
    trip_files: list[Path],
    batch_size: int,
    insert_page_size: int,
) -> None:
    """
    Chunked load of Parquet -> raw.yellow_taxi_trips with idempotency.

    Idempotency strategy:
    - raw_trip_id is stable (md5(source_file + ':' + source_row_number))
    - INSERT uses ON CONFLICT (raw_trip_id) DO NOTHING
    # We intentionally use DO NOTHING because source files are treated as immutable.
    # For changed source files, local development should use --rebuild-raw.
    This allows safe re-runs without duplicating rows, while still preserving
    “raw fidelity” (no filtering of invalid records).
    """

    with conn.cursor() as cur:
        for f in trip_files:
            year, month = parse_year_month_from_filename(f)
            source_file = f.name

            # Infer + normalize column names from this file's schema.
            pf = pq.ParquetFile(f)
            arrow_schema = pf.schema_arrow
            src_cols = dedupe_column_names([to_snake_case(n) for n in arrow_schema.names])

            insert_cols = (
                src_cols
                + [
                    "raw_trip_id",
                    "source_file",
                    "source_year",
                    "source_month",
                    "source_row_number",
                ]
            )

            insert_sql = f"""
            INSERT INTO raw.yellow_taxi_trips ({", ".join(insert_cols)})
            VALUES %s
            ON CONFLICT (raw_trip_id) DO NOTHING;
            """

            row_offset = 0
            for batch in iter_parquet_batches(f, batch_size=batch_size):
                df = batch.to_pandas(types_mapper=None)
                df.columns = src_cols

                # Per-file row numbering must reflect the original record order.
                # We compute it using the running offset + row index in the batch.
                n = len(df)
                source_row_numbers = list(range(row_offset + 1, row_offset + n + 1))
                row_offset += n

                df["source_file"] = source_file
                df["source_year"] = year
                df["source_month"] = month
                df["source_row_number"] = source_row_numbers
                df["raw_trip_id"] = [
                    stable_raw_trip_id(source_file, rn) for rn in source_row_numbers
                ]

                rows = _df_to_rows(df, insert_cols)
                execute_values(
                    cur,
                    insert_sql,
                    rows,
                    page_size=insert_page_size,
                )
                conn.commit()


def load_zone_lookup(
    conn,
    zones_csv: Path,
    csv_chunk_size: int,
    insert_page_size: int,
) -> None:
    """
    Chunked load of taxi_zone_lookup.csv -> raw.taxi_zone_lookup.

    Idempotency strategy:
    - Primary key on location_id
    - Upsert to keep the lookup current across re-runs
    """

    with conn.cursor() as cur:
        for chunk in pd.read_csv(zones_csv, chunksize=csv_chunk_size):
            chunk.columns = dedupe_column_names([to_snake_case(c) for c in chunk.columns])
            if "location_id" not in chunk.columns:
                raise RuntimeError(
                    "Expected taxi_zone_lookup.csv to include LocationID / location_id column"
                )

            # Coerce location_id into an integer when possible; keep nulls as None.
            chunk["location_id"] = pd.to_numeric(chunk["location_id"], errors="coerce").astype(
                "Int64"
            )
            insert_cols = chunk.columns.tolist()

            # Build SET clause excluding the primary key.
            non_pk_cols = [c for c in insert_cols if c != "location_id"]
            set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_pk_cols]) or ""

            insert_sql = f"""
            INSERT INTO raw.taxi_zone_lookup ({", ".join(insert_cols)})
            VALUES %s
            ON CONFLICT (location_id) DO UPDATE SET
                {set_clause};
            """

            rows = _df_to_rows(chunk, insert_cols)
            execute_values(cur, insert_sql, rows, page_size=insert_page_size)
            conn.commit()

def validate_trip_file_schemas(trip_files: list[Path]) -> None:
    """
    Ensure all monthly Parquet files have the same source schema.

    This keeps raw ingestion explicit. If TLC changes the schema, we want to fail
    early instead of silently loading inconsistent data.
    """

    first_schema = pq.ParquetFile(trip_files[0]).schema_arrow
    first_cols = [to_snake_case(n) for n in first_schema.names]

    for f in trip_files[1:]:
        current_schema = pq.ParquetFile(f).schema_arrow
        current_cols = [to_snake_case(n) for n in current_schema.names]

        if current_cols != first_cols:
            raise RuntimeError(
                f"Schema mismatch detected in {f.name}. "
                f"Expected columns from {trip_files[0].name}, but found different columns."
            )


def run_validations(conn) -> None:
    """
    Print basic sanity checks required for the raw layer.
    """

    queries: list[tuple[str, str]] = [
        (
            "Total trip row count",
            "SELECT COUNT(*) AS trip_row_count FROM raw.yellow_taxi_trips;",
        ),
        (
            "Trip row count by source_file",
            """
            SELECT source_file, COUNT(*) AS row_count
            FROM raw.yellow_taxi_trips
            GROUP BY source_file
            ORDER BY source_file;
            """,
        ),
        (
            "Zone lookup row count",
            "SELECT COUNT(*) AS zone_row_count FROM raw.taxi_zone_lookup;",
        ),
        (
            "Duplicate check: (source_file, source_row_number)",
            """
            SELECT COUNT(*) AS duplicate_key_groups
            FROM (
                SELECT source_file, source_row_number, COUNT(*) AS n
                FROM raw.yellow_taxi_trips
                GROUP BY source_file, source_row_number
                HAVING COUNT(*) > 1
            ) d;
            """,
        ),
        (
    "Raw trip id uniqueness check",
    """
    SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT raw_trip_id) AS distinct_raw_trip_ids,
        COUNT(DISTINCT source_file || ':' || source_row_number::text) AS distinct_source_rows
    FROM raw.yellow_taxi_trips;
    """,
),
    ]

    with conn.cursor() as cur:
        for title, sql in queries:
            print(f"\n=== {title} ===")
            cur.execute(sql)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
            if len(colnames) == 1 and len(rows) == 1:
                print(f"{colnames[0]}: {rows[0][0]}")
                continue
            # Pretty-print a small table without extra dependencies.
            print(" | ".join(colnames))
            print("-+-".join(["-" * len(c) for c in colnames]))
            for r in rows:
                print(" | ".join("" if v is None else str(v) for v in r))


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw NYC taxi data into Postgres.")
    parser.add_argument(
        "--raw-dir",
        default=str(RAW_DIR_DEFAULT),
        help="Directory containing raw source files (default: data/raw).",
    )
    parser.add_argument(
        "--trips-glob",
        default=TRIPS_GLOB_DEFAULT,
        help="Glob for monthly trip parquet files (default: yellow_tripdata_*.parquet).",
    )
    parser.add_argument(
        "--zones-file",
        default=ZONES_FILE_DEFAULT,
        help="Taxi zone lookup CSV filename (default: taxi_zone_lookup.csv).",
    )
    parser.add_argument(
        "--rebuild-raw",
        action="store_true",
        help="Drop and recreate the raw schema before loading (local dev only).",
    )
    parser.add_argument(
        "--parquet-batch-size",
        type=int,
        default=100_000,
        help="Arrow record batch size for Parquet streaming (default: 100000).",
    )
    parser.add_argument(
        "--csv-chunk-size",
        type=int,
        default=10_000,
        help="Row chunk size for CSV streaming (default: 10000).",
    )
    parser.add_argument(
        "--insert-page-size",
        type=int,
        default=5_000,
        help="Page size for psycopg2 execute_values batching (default: 5000).",
    )
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    trip_files = get_trip_files(raw_dir, args.trips_glob)
    zones_csv = raw_dir / args.zones_file
    if not zones_csv.exists():
        raise FileNotFoundError(f"Zone lookup CSV not found at {zones_csv}")

    cfg = load_pg_config_from_env()
    with connect(cfg) as conn:
        if args.rebuild_raw:
            print("Rebuilding schema raw (DROP SCHEMA ... CASCADE).")
            rebuild_raw_schema(conn)
        else:
            ensure_raw_schema(conn)

        validate_trip_file_schemas(trip_files)
        ensure_raw_tables(conn, sample_trip_file=trip_files[0], zones_csv=zones_csv)

        started_at = dt.datetime.now(dt.timezone.utc)
        print(f"Starting trips load at {started_at.isoformat()} for {len(trip_files)} files.")
        load_trips(
            conn,
            trip_files=trip_files,
            batch_size=args.parquet_batch_size,
            insert_page_size=args.insert_page_size,
        )

        print("Loading taxi zone lookup.")
        load_zone_lookup(
            conn,
            zones_csv=zones_csv,
            csv_chunk_size=args.csv_chunk_size,
            insert_page_size=args.insert_page_size,
        )

        finished_at = dt.datetime.now(dt.timezone.utc)
        print(f"Finished ingestion at {finished_at.isoformat()}.")

        run_validations(conn)


if __name__ == "__main__":
    main()

