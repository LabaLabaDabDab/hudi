#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Micro-benchmark for Hudi Spark writes: compare hoodie.index.type variants.

Spark-supported index types (see SparkHoodieIndexFactory / HoodieIndex.IndexType):
  SIMPLE, INMEMORY, BLOOM, GLOBAL_BLOOM, GLOBAL_SIMPLE, BUCKET,
  GLOBAL_RECORD_LEVEL_INDEX, RECORD_LEVEL_INDEX, RADIX_SPLINE

Not exercised here:
  FLINK_STATE — Flink-only
  RECORD_INDEX — deprecated alias for the global record index (same engine as GLOBAL_RECORD_LEVEL_INDEX)

Env:
  HUDI_BASE_ROOT — default file:///tmp/hudi_bench/trips_cow if unset.
  HUDI_SPARK_JARS — comma-separated paths to Hudi fat jar(s); required unless you use spark-submit --jars.
  HUDI_TABLE_NAME, N_INITIAL, N_UPDATES, N_INSERTS, ROUNDS
  HUDI_INDEX_FILTER — comma-separated subset of profile ids (default: all)
  HUDI_BUCKET_NUM_BUCKETS — for BUCKET profiles (default: 64)

Docker Compose (this repo, docker-compose.yml):
  Namenode RPC is published on the host as localhost:9000; Web UI http://localhost:9870 .
  Service hudi-dev sets HUDI_BASE_ROOT=hdfs://namenode:9000/user/hudi/trips_cow for in-network access.
  If you run Spark on the host (not in a container), use:
    export HUDI_BASE_ROOT=hdfs://localhost:9000/user/hudi/trips_cow
  and point Spark at the same default FS, e.g.:
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000

Hudi JAR (required — plain Spark does not include Hudi):
  Build bundle in this repo, then either:

    export HUDI_SPARK_JARS="$PWD/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-"*.jar
    spark-submit baseline_benchmark.py

  (Шаблон с * можно передать в кавычках — скрипт сам раскроет glob в Python;
  zsh иначе может оставить буквальную строку .../*.jar и Spark не найдёт файл.)

  or:

    spark-submit --jars /path/to/hudi-spark3.5-bundle_2.12-1.1.1.jar baseline_benchmark.py

  HUDI_SPARK_JARS may list several jars separated by commas. The bundle major Spark/Scala
  line must match your Spark (e.g. Spark 3.5 + Scala 2.12 vs Homebrew Spark 4.x — versions
  must be compatible or you will get runtime errors after the class loads).
"""

from __future__ import annotations

import glob
import os
import sys
import time
import traceback
from statistics import median
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Default local path for spark-submit without HDFS; set HUDI_BASE_ROOT=hdfs://... on cluster.
BASE_ROOT = os.environ.get("HUDI_BASE_ROOT", "file:///tmp/hudi_bench/trips_cow")
TABLE_NAME = os.environ.get("HUDI_TABLE_NAME", "trips_cow")

N_INITIAL = int(os.environ.get("N_INITIAL", "1000000"))
N_UPDATES = int(os.environ.get("N_UPDATES", "100000"))
N_INSERTS = int(os.environ.get("N_INSERTS", "100000"))
ROUNDS = int(os.environ.get("ROUNDS", "3"))
BUCKET_NUM = int(os.environ.get("HUDI_BUCKET_NUM_BUCKETS", "64"))

# Comma-separated profile ids; empty = all
_INDEX_FILTER_RAW = os.environ.get("HUDI_INDEX_FILTER", "").strip()
INDEX_FILTER: Optional[set[str]] = None
if _INDEX_FILTER_RAW:
    INDEX_FILTER = {x.strip() for x in _INDEX_FILTER_RAW.split(",") if x.strip()}

_SPARK: Optional[SparkSession] = None


def _is_auxiliary_maven_jar(path: str) -> bool:
    """Skip *-sources.jar / *-javadoc.jar from globs (they are not runnable bundles)."""
    name = os.path.basename(path)
    return name.endswith("-sources.jar") or name.endswith("-javadoc.jar")


def _resolve_hudi_spark_jars(jars_env: str) -> str:
    """
    Turn HUDI_SPARK_JARS into comma-separated existing paths.
    Expands globs in Python so zsh/bash do not leave a literal '*'.
    """
    resolved: List[str] = []
    for raw in jars_env.split(","):
        token = raw.strip()
        if not token:
            continue
        if any(c in token for c in "*?["):
            matches = sorted(glob.glob(token))
            if not matches:
                print(
                    f"\nОшибка: HUDI_SPARK_JARS — шаблон не совпал ни с одним файлом:\n  {token!r}\n"
                    "Соберите bundle: mvn -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am -DskipTests package\n",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            bundle_matches = [m for m in matches if not _is_auxiliary_maven_jar(m)]
            if not bundle_matches:
                print(
                    f"\nОшибка: после шаблона {token!r} остались только *-sources.jar / *-javadoc.jar.\n"
                    "Нужен основной fat-jar: hudi-spark3.5-bundle_2.12-<version>.jar (без -sources).\n",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            resolved.extend(bundle_matches)
        else:
            if not os.path.isfile(token):
                print(
                    f"\nОшибка: HUDI_SPARK_JARS — файл не найден:\n  {token!r}\n",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            rp = os.path.realpath(token)
            if _is_auxiliary_maven_jar(rp):
                print(
                    f"\nОшибка: указан вспомогательный артефакт, а не bundle:\n  {rp}\n"
                    "Используйте hudi-spark3.5-bundle_2.12-<version>.jar без суффикса -sources.\n",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            resolved.append(rp)
    seen: set[str] = set()
    uniq: List[str] = []
    for p in resolved:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return ",".join(uniq)


def init_spark() -> SparkSession:
    """Create SparkSession; attach Hudi jars from HUDI_SPARK_JARS if set (comma-separated)."""
    global _SPARK
    if _SPARK is not None:
        return _SPARK
    builder = SparkSession.builder.appName("hudi-baseline-benchmark")
    jars_raw = os.environ.get("HUDI_SPARK_JARS", "").strip()
    if jars_raw:
        jars = _resolve_hudi_spark_jars(jars_raw)
        if jars:
            builder = builder.config("spark.jars", jars)
    _SPARK = builder.getOrCreate()
    _SPARK.sparkContext.setLogLevel("WARN")
    return _SPARK


def active_spark() -> SparkSession:
    if _SPARK is None:
        raise RuntimeError("init_spark() was not called")
    return _SPARK


def require_hudi_or_exit() -> None:
    """Fail fast if Hudi datasource is not visible to Spark's classloader (not the JVM system loader)."""
    spark = active_spark()
    jvm = spark._jvm
    # Py4J's plain Class.forName() uses the wrong loader; jars from spark.jars live on Spark's URLClassLoader.
    loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    try:
        jvm.java.lang.Class.forName("org.apache.hudi.DefaultSource", True, loader)
    except Exception:
        print(
            "\nОшибка: для Spark classloader не виден org.apache.hudi.DefaultSource.\n"
            "Если JAR уже в spark.jars, это редко — сообщите; чаще Hudi просто не подключён.\n\n"
            "Соберите bundle:\n"
            "  mvn -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am -DskipTests package\n\n"
            "Укажите основной JAR (не *-sources.jar):\n"
            "  export HUDI_SPARK_JARS=$PWD/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.1.1.jar\n"
            "  spark-submit baseline_benchmark.py\n\n"
            "Важно: у вас Spark 4.x из Homebrew, а bundle — для Spark 3.5. Проверка класса может пройти,\n"
            "но запись в Hudi дальше может упасть из‑за несовместимости ABI — надёжнее Spark 3.5 + тот же bundle\n"
            "(например Spark из docker-compose в этом репозитории).\n",
            file=sys.stderr,
        )
        raise SystemExit(2)

common_options: Dict[str, str] = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "dt",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    # Metadata table is on by default; explicit for record-level index profiles
    "hoodie.metadata.enable": "true",
}


def spark_index_profiles() -> List[Dict[str, Any]]:
    """
    Extra write options per index. Keys must match SparkHoodieIndexFactory switch cases.
    """
    md_global_on = {
        "hoodie.metadata.global.record.level.index.enable": "true",
        "hoodie.metadata.record.level.index.enable": "false",
    }
    md_partition_rli_on = {
        "hoodie.metadata.global.record.level.index.enable": "false",
        "hoodie.metadata.record.level.index.enable": "true",
    }
    return [
        {
            "id": "SIMPLE",
            "label": "SIMPLE (partition-scoped join index)",
            "options": {"hoodie.index.type": "SIMPLE"},
        },
        {
            "id": "INMEMORY",
            "label": "INMEMORY",
            "options": {"hoodie.index.type": "INMEMORY"},
        },
        {
            "id": "BLOOM",
            "label": "BLOOM",
            "options": {"hoodie.index.type": "BLOOM"},
        },
        {
            "id": "GLOBAL_BLOOM",
            "label": "GLOBAL_BLOOM",
            "options": {"hoodie.index.type": "GLOBAL_BLOOM"},
        },
        {
            "id": "GLOBAL_SIMPLE",
            "label": "GLOBAL_SIMPLE",
            "options": {"hoodie.index.type": "GLOBAL_SIMPLE"},
        },
        {
            "id": "BUCKET_SIMPLE",
            "label": "BUCKET (engine=SIMPLE)",
            "options": {
                "hoodie.index.type": "BUCKET",
                "hoodie.index.bucket.engine": "SIMPLE",
                "hoodie.bucket.index.num.buckets": str(BUCKET_NUM),
            },
        },
        {
            "id": "BUCKET_CONSISTENT_HASHING",
            "label": "BUCKET (engine=CONSISTENT_HASHING)",
            "options": {
                "hoodie.index.type": "BUCKET",
                "hoodie.index.bucket.engine": "CONSISTENT_HASHING",
                "hoodie.bucket.index.num.buckets": str(BUCKET_NUM),
            },
        },
        {
            "id": "GLOBAL_RECORD_LEVEL_INDEX",
            "label": "GLOBAL_RECORD_LEVEL_INDEX (metadata record index, global keys)",
            "options": {
                "hoodie.index.type": "GLOBAL_RECORD_LEVEL_INDEX",
                **md_global_on,
            },
        },
        {
            "id": "RECORD_LEVEL_INDEX",
            "label": "RECORD_LEVEL_INDEX (metadata record index, partition-scoped keys)",
            "options": {
                "hoodie.index.type": "RECORD_LEVEL_INDEX",
                **md_partition_rli_on,
            },
        },
        {
            "id": "RADIX_SPLINE",
            "label": "RADIX_SPLINE",
            "options": {"hoodie.index.type": "RADIX_SPLINE"},
        },
    ]


def delete_path_if_exists(path_str: str) -> None:
    """Resolve FileSystem from the path URI (hdfs vs file); default FS alone would mismatch."""
    spark = active_spark()
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hadoop_conf)
    if fs.exists(path):
        fs.delete(path, True)


def timed_write(
    df,
    operation: str,
    mode: str,
    path: str,
    extra_options: Optional[Dict[str, str]] = None,
) -> float:
    opts = dict(common_options)
    if extra_options:
        opts.update(extra_options)

    t0 = time.perf_counter()
    (
        df.write.format("hudi")
        .options(**opts)
        .option("hoodie.datasource.write.operation", operation)
        .mode(mode)
        .save(path)
    )
    t1 = time.perf_counter()
    return t1 - t0


def timed_action(label: str, fn: Callable[[], Any]) -> Dict[str, Any]:
    t0 = time.perf_counter()
    result = fn()
    t1 = time.perf_counter()
    return {
        "label": label,
        "seconds": round(t1 - t0, 2),
        "result": result,
    }


def build_initial_df():
    spark = active_spark()
    return (
        spark.range(0, N_INITIAL)
        .withColumn("ts", expr("id"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('v_', id)"))
    )


def build_scattered_updates_df():
    spark = active_spark()
    return (
        spark.range(0, N_UPDATES)
        .withColumn("id", expr(f"pmod(id * 15485863, {N_INITIAL})"))
        .withColumn("ts", expr("id + 100000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('updated_', id)"))
    )


def build_new_inserts_df():
    spark = active_spark()
    return (
        spark.range(N_INITIAL, N_INITIAL + N_INSERTS)
        .withColumn("ts", expr("id + 200000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('new_', id)"))
    )


def build_mixed_df():
    spark = active_spark()
    existing_part = (
        spark.range(0, N_UPDATES // 2)
        .withColumn("id", expr(f"pmod(id * 15485863, {N_INITIAL})"))
        .withColumn("ts", expr("id + 300000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('mixed_existing_', id)"))
    )

    new_part = (
        spark.range(N_INITIAL + N_INSERTS, N_INITIAL + N_INSERTS + (N_UPDATES // 2))
        .withColumn("ts", expr("id + 400000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('mixed_new_', id)"))
    )

    return existing_part.unionByName(new_part)


def round_path(profile_id: str, round_no: int) -> str:
    safe = profile_id.replace(" ", "_")
    return f"{BASE_ROOT.rstrip('/')}_{safe}_round_{round_no}"


def run_one_round(profile: Dict[str, Any], round_no: int) -> Dict[str, Any]:
    spark = active_spark()
    spark.catalog.clearCache()

    path = round_path(profile["id"], round_no)
    delete_path_if_exists(path)

    extra = profile["options"]
    metrics: Dict[str, Any] = {
        "profile_id": profile["id"],
        "profile_label": profile["label"],
        "round": round_no,
        "base_path": path,
        "ok": True,
        "error": None,
    }

    try:
        initial_df = build_initial_df()
        bulk_insert_seconds = timed_write(initial_df, "bulk_insert", "overwrite", path, extra)
        metrics["bulk_insert_seconds"] = round(bulk_insert_seconds, 2)
        metrics["bulk_insert_rows_per_sec"] = round(N_INITIAL / bulk_insert_seconds, 2)

        updates_df = build_scattered_updates_df()
        upsert_seconds = timed_write(updates_df, "upsert", "append", path, extra)
        metrics["upsert_existing_seconds"] = round(upsert_seconds, 2)
        metrics["upsert_existing_rows_per_sec"] = round(N_UPDATES / upsert_seconds, 2)

        inserts_df = build_new_inserts_df()
        insert_seconds = timed_write(inserts_df, "insert", "append", path, extra)
        metrics["insert_new_seconds"] = round(insert_seconds, 2)
        metrics["insert_new_rows_per_sec"] = round(N_INSERTS / insert_seconds, 2)

        mixed_df = build_mixed_df()
        mixed_seconds = timed_write(mixed_df, "upsert", "append", path, extra)
        metrics["mixed_upsert_seconds"] = round(mixed_seconds, 2)
        metrics["mixed_upsert_rows_per_sec"] = round(N_UPDATES / mixed_seconds, 2)

        result_df = spark.read.format("hudi").load(path)

        count_metric = timed_action("snapshot_count", lambda: result_df.count())
        metrics["snapshot_count_seconds"] = count_metric["seconds"]
        metrics["final_count"] = count_metric["result"]

        updated_check = timed_action(
            "updated_payload_check",
            lambda: result_df.filter("payload like 'updated_%'").count(),
        )
        metrics["updated_payload_rows"] = updated_check["result"]
        metrics["updated_payload_check_seconds"] = updated_check["seconds"]

        new_check = timed_action(
            "new_payload_check",
            lambda: result_df.filter("payload like 'new_%'").count(),
        )
        metrics["new_payload_rows"] = new_check["result"]
        metrics["new_payload_check_seconds"] = new_check["seconds"]

        mixed_existing_check = timed_action(
            "mixed_existing_payload_check",
            lambda: result_df.filter("payload like 'mixed_existing_%'").count(),
        )
        metrics["mixed_existing_payload_rows"] = mixed_existing_check["result"]
        metrics["mixed_existing_payload_check_seconds"] = mixed_existing_check["seconds"]

        mixed_new_check = timed_action(
            "mixed_new_payload_check",
            lambda: result_df.filter("payload like 'mixed_new_%'").count(),
        )
        metrics["mixed_new_payload_rows"] = mixed_new_check["result"]
        metrics["mixed_new_payload_check_seconds"] = mixed_new_check["seconds"]

    except Exception:
        metrics["ok"] = False
        metrics["error"] = traceback.format_exc()
        # So medians/summary can skip or show NaN
        for k in (
            "bulk_insert_rows_per_sec",
            "upsert_existing_rows_per_sec",
            "insert_new_rows_per_sec",
            "mixed_upsert_rows_per_sec",
            "snapshot_count_seconds",
        ):
            metrics[k] = None

    return metrics


def _median_nums(values: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return float(median(nums))


def print_comparison_table(summaries: List[Dict[str, Any]]) -> None:
    cols = (
        "profile_id",
        "bulk_rps",
        "upsert_rps",
        "insert_rps",
        "mixed_rps",
        "snap_s",
        "ok",
    )
    rows = []
    for s in summaries:
        rows.append(
            [
                s["profile_id"],
                s.get("median_bulk_insert_rows_per_sec"),
                s.get("median_upsert_existing_rows_per_sec"),
                s.get("median_insert_new_rows_per_sec"),
                s.get("median_mixed_upsert_rows_per_sec"),
                s.get("median_snapshot_count_seconds"),
                s.get("all_rounds_ok"),
            ]
        )

    def fmt_cell(x: Any) -> str:
        if x is None:
            return "-"
        if isinstance(x, float):
            return f"{x:.2f}"
        return str(x)

    widths = [max(len(c), *(len(fmt_cell(r[i])) for r in rows)) for i, c in enumerate(cols)]
    header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    sep = "-+-".join("-" * w for w in widths)
    print("\n=== Comparison (median rows/sec where applicable; snap_s = snapshot count time) ===")
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(fmt_cell(r[i]).ljust(widths[i]) for i in range(len(cols))))


def main() -> int:
    init_spark()
    require_hudi_or_exit()

    profiles = spark_index_profiles()
    if INDEX_FILTER is not None:
        profiles = [p for p in profiles if p["id"] in INDEX_FILTER]
        missing = INDEX_FILTER - {p["id"] for p in profiles}
        if missing:
            print(f"WARNING: unknown HUDI_INDEX_FILTER ids ignored: {sorted(missing)}", file=sys.stderr)

    if not profiles:
        print("No profiles to run (check HUDI_INDEX_FILTER).", file=sys.stderr)
        return 2

    print(
        f"Benchmark: {len(profiles)} index profile(s), {ROUNDS} round(s), "
        f"initial={N_INITIAL}, updates={N_UPDATES}, inserts={N_INSERTS}, base={BASE_ROOT}"
    )
    for p in profiles:
        print(f"  - {p['id']}: {p['label']}")

    all_by_profile: Dict[str, List[Dict[str, Any]]] = {p["id"]: [] for p in profiles}

    for p in profiles:
        for r in range(1, ROUNDS + 1):
            m = run_one_round(p, r)
            all_by_profile[p["id"]].append(m)
            status = "OK" if m["ok"] else "FAIL"
            print(f"[{status}] {p['id']} round {r}: {m}")

    summaries: List[Dict[str, Any]] = []
    for p in profiles:
        runs = all_by_profile[p["id"]]
        summary: Dict[str, Any] = {
            "profile_id": p["id"],
            "profile_label": p["label"],
            "rounds": ROUNDS,
            "base_root": BASE_ROOT,
            "table_name": TABLE_NAME,
            "n_initial": N_INITIAL,
            "n_updates": N_UPDATES,
            "n_inserts": N_INSERTS,
            "median_bulk_insert_rows_per_sec": _median_nums([x.get("bulk_insert_rows_per_sec") for x in runs]),
            "median_upsert_existing_rows_per_sec": _median_nums(
                [x.get("upsert_existing_rows_per_sec") for x in runs]
            ),
            "median_insert_new_rows_per_sec": _median_nums([x.get("insert_new_rows_per_sec") for x in runs]),
            "median_mixed_upsert_rows_per_sec": _median_nums(
                [x.get("mixed_upsert_rows_per_sec") for x in runs]
            ),
            "median_snapshot_count_seconds": _median_nums([x.get("snapshot_count_seconds") for x in runs]),
            "all_rounds_ok": all(x.get("ok") for x in runs),
            "last_final_count": runs[-1].get("final_count"),
            "last_updated_payload_rows": runs[-1].get("updated_payload_rows"),
            "last_new_payload_rows": runs[-1].get("new_payload_rows"),
            "last_mixed_existing_payload_rows": runs[-1].get("mixed_existing_payload_rows"),
            "last_mixed_new_payload_rows": runs[-1].get("mixed_new_payload_rows"),
        }
        summaries.append(summary)

    print("\n=== SUMMARY (per index) ===")
    for s in summaries:
        print(s)

    print_comparison_table(summaries)

    print(
        "\nNote: RECORD_INDEX enum is deprecated; use GLOBAL_RECORD_LEVEL_INDEX "
        "(same Spark engine class). FLINK_STATE is not applicable to Spark."
    )

    return 0 if all(s["all_rounds_ok"] for s in summaries) else 1


if __name__ == "__main__":
    raise SystemExit(main())
