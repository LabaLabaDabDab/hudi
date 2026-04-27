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
  HUDI_BENCH_PARTITION_BUCKETS — number of distinct partition paths (default 100): synthetic names p000…p{N-1}
    under partition field dt (not calendar dates; avoids day-of-month limit when N > 31).
  HUDI_KEY_DISTRIBUTION — synthetic record key shape: linear, quadratic, affine7919, triangular, poly_sum
  BENCH_SPARK_SHUFFLE_PARTITIONS, BENCH_SPARK_DEFAULT_PARALLELISM — optional Spark tuning for large N_*
  BENCH_SPARK_DRIVER_MAX_RESULT_SIZE, BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD — optional (e.g. multi-million rows)
  BENCH_SPARK_EXECUTOR_MEMORY, BENCH_SPARK_DRIVER_MEMORY — passed through spark-submit (Docker large runs)
  HUDI_INDEX_FILTER — comma-separated profile ids, or ALL / empty for all COW-safe profiles
    (excludes BUCKET_CONSISTENT_HASHING). Adds RECORD_INDEX alongside GLOBAL_RECORD_LEVEL_INDEX.
  HUDI_BUCKET_NUM_BUCKETS — for BUCKET profiles (default: 64)
  BUCKET + CONSISTENT_HASHING — not valid for COPY_ON_WRITE (benchmark default); use MOR or only SIMPLE bucket engine.
  HUDI_CLEANUP_AFTER_PROFILE — delete all round paths for profile after it finishes (default: true)
  HUDI_BENCH_LOG_LEVEL — Spark log level (default: WARN), e.g. INFO/DEBUG
  HUDI_BENCH_JSON_SUMMARY — if set, path to a JSON file written at the end (all per-index summaries, n_*, etc.)
  HUDI_RADIX_PROFILE_TAG_LOCATION — if true, sets hoodie.index.radix_spline.profile_tag_location=true
    for the RADIX_SPLINE profile so each Spark task logs a timing breakdown (encode / reader / lookup / entry read).
    Use with HUDI_BENCH_LOG_LEVEL=INFO. Rebuild the Hudi bundle after changing Java code.
  HUDI_RADIX_LOOKUP_WINDOW_KEYS — optional hoodie.index.radix_spline.lookup_window_keys
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE — if true, hoodie.index.radix_spline.lookup_window_adaptive=true
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN, HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX,
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS — optional when adaptive is on
  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE — if true/false, sets hoodie.metadata.index.radix_spline.enable
    (A/B vs Java default). Omit to use the default from the Hudi JAR.

Docker Compose (this repo, docker-compose.yml):
  Image Spark 3.5.7 + Scala 2.12 + Java 17 matches hudi-spark3.5-bundle_2.12. From repo root:
    ./run-docker-baseline-benchmark.sh
  Or inside hudi-dev after compose up:
    /workspace/hudi/scripts/run-baseline-benchmark-in-container.sh
  Compare several index types (Docker, COW-safe list; tune N_* on the host):
    ./run-docker-index-comparison.sh
  Namenode RPC is published on the host as localhost:9000; Web UI http://localhost:9870 .
  Service hudi-dev sets HUDI_BASE_ROOT=hdfs://namenode:9000/user/hudi/trips_cow for in-network access.
  If you run Spark on the host (not in a container), use:
    export HUDI_BASE_ROOT=hdfs://localhost:9000/user/hudi/trips_cow
  and point Spark at the same default FS, e.g.:
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000

Hudi JAR (required — plain Spark does not include Hudi):
  Build bundle in this repo (Maven for -Dspark3.5 -Dscala-2.12 must run on JDK 17 — set JAVA_HOME), then:

    export HUDI_SPARK_JARS="$PWD/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-"*.jar
    spark-submit baseline_benchmark.py

  Spark 4.x + Scala 2.13 (e.g. Homebrew Spark) — соберите bundle с профилями проекта (JDK 17 для Maven), затем:

    mvn -Dspark4.0 -Dscala-2.13 -pl packaging/hudi-spark-bundle -am -DskipTests package
    export HUDI_SPARK_JARS="$PWD/packaging/hudi-spark-bundle/target/hudi-spark4.0-bundle_2.13-"*.jar

  (Шаблон с * можно передать в кавычках — скрипт сам раскроет glob в Python;
  zsh иначе может оставить буквальную строку .../*.jar и Spark не найдёт файл.)

  or:

    spark-submit --jars /path/to/hudi-spark3.5-bundle_2.12-1.1.1.jar baseline_benchmark.py

  With spark-submit, pass the same bundle via --jars (not only HUDI_SPARK_JARS): some setups
  never put the datasource on the driver URLClassLoader unless the jar is on the submit line.

  HUDI_SPARK_JARS may list several jars separated by commas. The bundle major Spark/Scala
  line must match your Spark (e.g. Spark 3.5 + Scala 2.12 vs Homebrew Spark 4.x — versions
  must be compatible or you will get runtime errors after the class loads).
"""

from __future__ import annotations

import glob
import json
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

N_INITIAL = int(os.environ.get("N_INITIAL", "5000000"))
N_UPDATES = int(os.environ.get("N_UPDATES", "1250000"))
N_INSERTS = int(os.environ.get("N_INSERTS", "500000"))
ROUNDS = int(os.environ.get("ROUNDS", "3"))
BUCKET_NUM = int(os.environ.get("HUDI_BUCKET_NUM_BUCKETS", "64"))
CLEANUP_AFTER_PROFILE = os.environ.get("HUDI_CLEANUP_AFTER_PROFILE", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)
BENCH_LOG_LEVEL = os.environ.get("HUDI_BENCH_LOG_LEVEL", "WARN").strip().upper()
RADIX_PROFILE_TAG_LOCATION = os.environ.get("HUDI_RADIX_PROFILE_TAG_LOCATION", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)
RADIX_MAX_ERROR = os.environ.get("HUDI_RADIX_MAX_ERROR", "").strip()
RADIX_BITS = os.environ.get("HUDI_RADIX_BITS", "").strip()
RADIX_LOOKUP_WINDOW_KEYS = os.environ.get("HUDI_RADIX_LOOKUP_WINDOW_KEYS", "").strip()
RADIX_LOOKUP_WINDOW_ADAPTIVE = os.environ.get("HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)
RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN = os.environ.get("HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN", "").strip()
RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX = os.environ.get("HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX", "").strip()
RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS = os.environ.get(
    "HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS", ""
).strip()
KEY_DISTRIBUTION = os.environ.get("HUDI_KEY_DISTRIBUTION", "quadratic").strip().lower()

_PARTITION_BUCKETS_RAW = os.environ.get("HUDI_BENCH_PARTITION_BUCKETS", "100").strip()
try:
    PARTITION_BUCKET_COUNT = int(_PARTITION_BUCKETS_RAW)
except ValueError as e:
    raise SystemExit(
        f"HUDI_BENCH_PARTITION_BUCKETS must be an integer, got {_PARTITION_BUCKETS_RAW!r}"
    ) from e
if PARTITION_BUCKET_COUNT < 1:
    raise SystemExit(f"HUDI_BENCH_PARTITION_BUCKETS must be >= 1, got {PARTITION_BUCKET_COUNT}")

_METADATA_RADIX_ENABLE_RAW = os.environ.get("HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE", "").strip().lower()

# Comma-separated profile ids. Empty or ALL = all Spark index profiles safe for COPY_ON_WRITE
# (excludes BUCKET_CONSISTENT_HASHING — use MOR or run that profile explicitly).
_INDEX_FILTER_RAW = os.environ.get("HUDI_INDEX_FILTER", "").strip()
INDEX_FILTER_ALL_COW_SAFE = not _INDEX_FILTER_RAW or _INDEX_FILTER_RAW.upper() == "ALL"
INDEX_FILTER: Optional[set[str]] = None
if not INDEX_FILTER_ALL_COW_SAFE:
    INDEX_FILTER = {x.strip() for x in _INDEX_FILTER_RAW.split(",") if x.strip()}

COW_UNSAFE_PROFILE_IDS = frozenset({"BUCKET_CONSISTENT_HASHING"})

_SPARK: Optional[SparkSession] = None

RADIX_ARTIFACT_MAGIC = 0x52534958  # RSIX, see SimpleTempRadixArtifactWriter.MAGIC


def _spread_seed_sql(row_id_col: str) -> str:
    """Map monotonic row id -> [0, N_INITIAL) without 32-bit overflow in pmod."""
    return (
        f"pmod(cast({row_id_col} as bigint) * cast(15485863 as bigint), cast({N_INITIAL} as bigint))"
    )


def _partition_path_expr(seed_col: str) -> str:
    """
    Assign each row to one of PARTITION_BUCKET_COUNT synthetic partitions (field dt).

    Paths look like p000, p001, … so we are not limited to 31 calendar days in March.
    """
    n = PARTITION_BUCKET_COUNT
    width = max(len(str(n - 1)), 3)
    return (
        f"concat('p', lpad(cast(pmod(cast({seed_col} as bigint), cast({n} as bigint)) as string), "
        f"{width}, '0'))"
    )


def _key_expr(seed_col: str) -> str:
    """
    Build benchmark record-key expression from a monotonic seed column.
    All modes are strictly increasing in seed for seed >= 0, so keys stay unique
    across bulk / upserts / inserts for the same seed ranges.

    Expressions use bigint and integral ops only: RADIX_SPLINE rejects DOUBLE record keys
    (Spark SQL ``/`` promotes to floating point).

    - linear: id = seed
    - quadratic: id = seed^2 (dense low keys)
    - affine7919: id = seed * 7919 (spread, still injective for bounded seed)
    - triangular: id = seed * (seed + 1) / 2 (integer div)
    - poly_sum: id = seed^2 + seed
    """
    s = f"cast({seed_col} as bigint)"
    if KEY_DISTRIBUTION == "linear":
        return s
    if KEY_DISTRIBUTION == "quadratic":
        return f"({s} * {s})"
    if KEY_DISTRIBUTION == "affine7919":
        return f"({s} * cast(7919 as bigint))"
    if KEY_DISTRIBUTION == "triangular":
        return f"div({s} * ({s} + cast(1 as bigint)), cast(2 as bigint))"
    if KEY_DISTRIBUTION == "poly_sum":
        return f"(({s} * {s}) + {s})"
    raise ValueError(
        f"Unsupported HUDI_KEY_DISTRIBUTION={KEY_DISTRIBUTION!r}. "
        "Use one of: linear, quadratic, affine7919, triangular, poly_sum."
    )


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
                    "Соберите bundle под JDK 17 (JAVA_HOME). Пример macOS:\n"
                    "  export JAVA_HOME=\"$(/usr/libexec/java_home -v 17)\"\n"
                    "  mvn -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am -DskipTests package\n",
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
    _shuffle = os.environ.get("BENCH_SPARK_SHUFFLE_PARTITIONS", "").strip()
    if _shuffle:
        builder = builder.config("spark.sql.shuffle.partitions", _shuffle)
    _par = os.environ.get("BENCH_SPARK_DEFAULT_PARALLELISM", "").strip()
    if _par:
        builder = builder.config("spark.default.parallelism", _par)
    _mem = os.environ.get("BENCH_SPARK_DRIVER_MAX_RESULT_SIZE", "").strip()
    if _mem:
        builder = builder.config("spark.driver.maxResultSize", _mem)
    _overhead = os.environ.get("BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD", "").strip()
    if _overhead:
        builder = builder.config("spark.executor.memoryOverhead", _overhead)
    _SPARK = builder.getOrCreate()
    _SPARK.sparkContext.setLogLevel(BENCH_LOG_LEVEL)
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
    loaders = []
    try:
        loaders.append(jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader())
    except Exception:
        pass
    try:
        loaders.append(jvm.java.lang.Thread.currentThread().getContextClassLoader())
    except Exception:
        pass
    last_err: Optional[BaseException] = None
    for loader in loaders:
        try:
            jvm.java.lang.Class.forName("org.apache.hudi.DefaultSource", True, loader)
            return
        except Exception as e:
            last_err = e
            continue
    print(
        "\nОшибка: для Spark classloader не виден org.apache.hudi.DefaultSource.\n"
        "Если JAR уже в spark.jars, это редко — сообщите; чаще Hudi просто не подключён.\n\n"
        "Соберите bundle под JDK 17 (JAVA_HOME). Пример macOS:\n"
        "  export JAVA_HOME=\"$(/usr/libexec/java_home -v 17)\"\n"
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
if _METADATA_RADIX_ENABLE_RAW in ("1", "true", "yes", "y", "on"):
    common_options["hoodie.metadata.index.radix_spline.enable"] = "true"
elif _METADATA_RADIX_ENABLE_RAW in ("0", "false", "no", "n", "off"):
    common_options["hoodie.metadata.index.radix_spline.enable"] = "false"


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
            "id": "RECORD_INDEX",
            "label": "RECORD_INDEX (deprecated enum; same Spark engine as GLOBAL_RECORD_LEVEL_INDEX)",
            "options": {
                "hoodie.index.type": "RECORD_INDEX",
                **md_global_on,
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
            "options": {
                **{"hoodie.index.type": "RADIX_SPLINE"},
                **(
                    {"hoodie.index.radix_spline.profile_tag_location": "true"}
                    if RADIX_PROFILE_TAG_LOCATION
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.max_error": RADIX_MAX_ERROR}
                    if RADIX_MAX_ERROR
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.radix_bits": RADIX_BITS}
                    if RADIX_BITS
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.lookup_window_keys": RADIX_LOOKUP_WINDOW_KEYS}
                    if RADIX_LOOKUP_WINDOW_KEYS
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.lookup_window_adaptive": "true"}
                    if RADIX_LOOKUP_WINDOW_ADAPTIVE
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.lookup_window_adaptive_min": RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN}
                    if RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN
                    else {}
                ),
                **(
                    {"hoodie.index.radix_spline.lookup_window_adaptive_max": RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX}
                    if RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX
                    else {}
                ),
                **(
                    {
                        "hoodie.index.radix_spline.lookup_window_adaptive_calibration_keys": RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS
                    }
                    if RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS
                    else {}
                ),
            },
        },
    ]


def _read_radix_artifact_header(dis) -> Dict[str, Any]:
    """Parse SimpleTempRadixArtifactWriter header via java.io.DataInputStream."""
    magic = dis.readInt()
    if magic != RADIX_ARTIFACT_MAGIC:
        return {"parse_error": f"bad_magic={magic:#x}"}
    version = dis.readInt()
    entry_count = dis.readLong()
    min_key = dis.readLong()
    max_key = dis.readLong()
    max_error = dis.readInt()
    radix_bits = dis.readInt()
    spline_len = dis.readInt()
    radix_len = dis.readInt()
    return {
        "artifact_format_version": version,
        "entry_count": entry_count,
        "min_key": min_key,
        "max_key": max_key,
        "max_error": max_error,
        "radix_bits": radix_bits,
        "spline_points": spline_len,
        "radix_buckets": radix_len,
    }


def collect_radix_artifact_stats(spark: SparkSession, table_base_path: str) -> Dict[str, Any]:
    """
    Summarize RADIX_SPLINE on-disk model: read each partition manifest under
    .hoodie/.radix_index_tmp/latest/*.properties and parse artifact binary headers.
    """
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    base = table_base_path.rstrip("/")
    latest = jvm.org.apache.hadoop.fs.Path(f"{base}/.hoodie/.radix_index_tmp/latest")
    fs = latest.getFileSystem(conf)
    if not fs.exists(latest):
        return {"error": "latest_manifest_dir_missing", "path": str(latest)}

    per_partition: List[Dict[str, Any]] = []
    try:
        statuses = fs.listStatus(latest)
    except Exception as e:
        return {"error": f"listStatus_failed:{e}"}

    for st in statuses:
        p = st.getPath()
        name = p.getName()
        if not name.endswith(".properties"):
            continue
        inp = None
        try:
            inp = fs.open(p)
            props = jvm.java.util.Properties()
            props.load(inp)
        except Exception as e:
            per_partition.append({"manifest": str(p), "error": f"manifest_load:{e}"})
            continue
        finally:
            if inp is not None:
                inp.close()

        artifact_uri = props.getProperty("artifactPath")
        if not artifact_uri:
            per_partition.append({"manifest": str(p), "error": "no_artifactPath_in_manifest"})
            continue

        ain = None
        try:
            ap = jvm.org.apache.hadoop.fs.Path(str(artifact_uri))
            afs = ap.getFileSystem(conf)
            if not afs.exists(ap):
                per_partition.append({"manifest": str(p), "artifact": artifact_uri, "error": "artifact_missing"})
                continue
            ain = afs.open(ap)
            dis = jvm.java.io.DataInputStream(ain)
            header = _read_radix_artifact_header(dis)
            header["manifest"] = str(p)
            header["artifact"] = artifact_uri
            per_partition.append(header)
        except Exception as e:
            per_partition.append({"manifest": str(p), "artifact": artifact_uri, "error": f"artifact_read:{e}"})
        finally:
            if ain is not None:
                ain.close()

    spline_counts = [x["spline_points"] for x in per_partition if "spline_points" in x]
    summary: Dict[str, Any] = {
        "manifest_files_seen": len(per_partition),
        "partitions_with_valid_spline_header": len(spline_counts),
        "spline_points_per_partition": spline_counts,
    }
    if spline_counts:
        summary["spline_points_min"] = min(spline_counts)
        summary["spline_points_max"] = max(spline_counts)
        spline_sum = sum(spline_counts)
        summary["spline_points_sum"] = spline_sum
        summary["total_spline_points_created"] = spline_sum
        summary["spline_points_avg"] = round(sum(spline_counts) / len(spline_counts), 2)
    return {"partitions": per_partition, "summary": summary}


def merged_radix_profile_write_options(profiles: List[Dict[str, Any]]) -> Dict[str, str]:
    radix = next((p for p in profiles if p["id"] == "RADIX_SPLINE"), None)
    if not radix:
        return {}
    merged = dict(common_options)
    merged.update(radix["options"])
    return merged


def effective_radix_spline_params(spark: SparkSession, profiles: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Effective hoodie.* radix-related keys for RADIX_SPLINE profile: explicit write options override
    HoodieIndexConfig JVM defaults where applicable; metadata radix toggles come from merged common_options.
    """
    merged = merged_radix_profile_write_options(profiles)
    if not merged:
        return {}
    try:
        defaults = fetch_hudi_radix_defaults_from_jvm(spark)
    except Exception:
        defaults = {}
    keys = sorted(set(k for k in list(merged.keys()) + list(defaults.keys()) if "radix" in k.lower()))
    out: Dict[str, str] = {}
    for k in keys:
        if k in merged:
            out[k] = merged[k]
        elif k in defaults:
            out[k] = defaults[k]
    return out


def fetch_hudi_radix_defaults_from_jvm(spark: SparkSession) -> Dict[str, str]:
    """HoodieIndexConfig defaults as strings (for options not overridden in the benchmark)."""
    jvm = spark._jvm
    H = jvm.org.apache.hudi.config.HoodieIndexConfig
    rows = [
        H.RADIX_SPLINE_INDEX_MAX_ERROR,
        H.RADIX_SPLINE_INDEX_RADIX_BITS,
        H.RADIX_SPLINE_MAX_ENTRIES_PER_PARTITION,
        H.RADIX_SPLINE_MERGE_MAX_ENTRIES_IN_MEMORY,
        H.RADIX_SPLINE_PROFILE_TAG_LOCATION,
        H.RADIX_SPLINE_LOOKUP_WINDOW_KEYS,
        H.RADIX_SPLINE_LOOKUP_WINDOW_ADAPTIVE,
        H.RADIX_SPLINE_LOOKUP_WINDOW_ADAPTIVE_MIN,
        H.RADIX_SPLINE_LOOKUP_WINDOW_ADAPTIVE_MAX,
        H.RADIX_SPLINE_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS,
    ]
    out: Dict[str, str] = {}
    for cp in rows:
        out[str(cp.key())] = str(cp.defaultValue())
    return out


def print_radix_configuration(spark: SparkSession, profiles: List[Dict[str, Any]]) -> None:
    radix = next((p for p in profiles if p["id"] == "RADIX_SPLINE"), None)
    if not radix:
        return

    print("\n=== RADIX_SPLINE configuration ===")
    print("Environment overrides (unset = not passed to write options):")
    print(f"  HUDI_RADIX_PROFILE_TAG_LOCATION={os.environ.get('HUDI_RADIX_PROFILE_TAG_LOCATION', '')!r}")
    print(f"  HUDI_RADIX_MAX_ERROR={os.environ.get('HUDI_RADIX_MAX_ERROR', '')!r}")
    print(f"  HUDI_RADIX_BITS={os.environ.get('HUDI_RADIX_BITS', '')!r}")
    print(f"  HUDI_RADIX_LOOKUP_WINDOW_KEYS={os.environ.get('HUDI_RADIX_LOOKUP_WINDOW_KEYS', '')!r}")
    print(f"  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE={os.environ.get('HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE', '')!r}")
    print(f"  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN={os.environ.get('HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN', '')!r}")
    print(f"  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX={os.environ.get('HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX', '')!r}")
    print(f"  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS={os.environ.get('HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS', '')!r}")
    print(f"  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE={os.environ.get('HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE', '')!r}")

    try:
        eff = effective_radix_spline_params(spark, profiles)
        print("\nEffective hoodie.* radix-related keys at benchmark start (write opts → else HoodieIndexConfig default):")
        for k in sorted(eff.keys()):
            print(f"  {k}={eff[k]}")
    except Exception as e:
        print(f"\n(could not compute effective radix params: {e})")

    print(
        "\nAfter each RADIX_SPLINE round: total spline points per partition manifest and the same "
        "effective radix map are printed from on-disk artifacts (header spline_points ≈ len(splineKeys))."
    )


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
        .withColumnRenamed("id", "seed")
        .withColumn("id", expr(_key_expr("seed")))
        .withColumn("ts", expr("id"))
        .withColumn("dt", expr(_partition_path_expr("seed")))
        .withColumn("payload", expr("concat('v_', id)"))
        .drop("seed")
    )


def build_scattered_updates_df():
    spark = active_spark()
    return (
        spark.range(0, N_UPDATES)
        .withColumnRenamed("id", "row_id")
        .withColumn("seed", expr(_spread_seed_sql("row_id")))
        .withColumn("id", expr(_key_expr("seed")))
        .withColumn("ts", expr("id + 100000000"))
        .withColumn("dt", expr(_partition_path_expr("seed")))
        .withColumn("payload", expr("concat('updated_', id)"))
        .drop("seed", "row_id")
    )


def build_new_inserts_df():
    spark = active_spark()
    return (
        spark.range(N_INITIAL, N_INITIAL + N_INSERTS)
        .withColumnRenamed("id", "seed")
        .withColumn("id", expr(_key_expr("seed")))
        .withColumn("ts", expr("id + 200000000"))
        .withColumn("dt", expr(_partition_path_expr("seed")))
        .withColumn("payload", expr("concat('new_', id)"))
        .drop("seed")
    )


def build_mixed_df():
    spark = active_spark()
    existing_part = (
        spark.range(0, N_UPDATES // 2)
        .withColumnRenamed("id", "row_id")
        .withColumn("seed", expr(_spread_seed_sql("row_id")))
        .withColumn("id", expr(_key_expr("seed")))
        .withColumn("ts", expr("id + 300000000"))
        .withColumn("dt", expr(_partition_path_expr("seed")))
        .withColumn("payload", expr("concat('mixed_existing_', id)"))
        .drop("seed", "row_id")
    )

    new_part = (
        spark.range(N_INITIAL + N_INSERTS, N_INITIAL + N_INSERTS + (N_UPDATES // 2))
        .withColumnRenamed("id", "seed")
        .withColumn("id", expr(_key_expr("seed")))
        .withColumn("ts", expr("id + 400000000"))
        .withColumn("dt", expr(_partition_path_expr("seed")))
        .withColumn("payload", expr("concat('mixed_new_', id)"))
        .drop("seed")
    )

    return existing_part.unionByName(new_part)


def round_path(profile_id: str, round_no: int) -> str:
    safe = profile_id.replace(" ", "_")
    return f"{BASE_ROOT.rstrip('/')}_{safe}_round_{round_no}"


def cleanup_profile_round_paths(profile_id: str) -> None:
    """Best-effort cleanup of all per-round table paths for one profile."""
    for r in range(1, ROUNDS + 1):
        try:
            delete_path_if_exists(round_path(profile_id, r))
        except Exception as e:
            print(f"WARNING: cleanup failed for {profile_id} round {r}: {e}", file=sys.stderr)


def run_one_round(profile: Dict[str, Any], round_no: int, all_profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
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

        if profile["id"] == "RADIX_SPLINE":
            eff = effective_radix_spline_params(spark, all_profiles)
            metrics["radix_effective_params"] = eff

            radix_info = collect_radix_artifact_stats(spark, path)
            metrics["radix_artifact_stats"] = radix_info

            print("\n=== RADIX_SPLINE: spline points & effective radix parameters ===")
            print("Effective hoodie.* radix-related options (explicit write → else HoodieIndexConfig default):")
            if eff:
                for k in sorted(eff.keys()):
                    print(f"  {k}={eff[k]}")
            else:
                print("  (none — RADIX_SPLINE profile not in filter?)")

            if radix_info.get("error"):
                print(f"\nOn-disk spline collect_failed: {radix_info!r}")
            else:
                summ = radix_info.get("summary", {})
                total_pts = summ.get("total_spline_points_created") or summ.get("spline_points_sum")
                n_parts = summ.get("partitions_with_valid_spline_header")
                print("\nSpline points (from artifact headers; one model per partition manifest):")
                print(f"  total_spline_points_created={total_pts}")
                print(f"  partitions_with_model={n_parts}")
                if summ.get("spline_points_min") is not None:
                    print(
                        f"  per_partition: min={summ.get('spline_points_min')} "
                        f"max={summ.get('spline_points_max')} avg={summ.get('spline_points_avg')}"
                    )

                metrics["radix_spline_points_total"] = total_pts
                metrics["radix_model_partition_count"] = n_parts

                parts = radix_info.get("partitions", [])
                sample = next((x for x in parts if x.get("spline_points") is not None), None)
                if sample:
                    print(
                        "\nSample partition artifact header (min_key/max_key/entry_count reflect stored index):"
                    )
                    print(
                        f"  max_error={sample.get('max_error')} radix_bits={sample.get('radix_bits')} "
                        f"radix_buckets={sample.get('radix_buckets')} "
                        f"entry_count={sample.get('entry_count')} "
                        f"min_key={sample.get('min_key')} max_key={sample.get('max_key')}"
                    )

                print("\nPer-partition details (first 25):")
                for row in parts[:25]:
                    if row.get("parse_error") or row.get("error"):
                        print(f"  {row}")
                    else:
                        print(
                            "  "
                            f"spline_points={row.get('spline_points')} "
                            f"entries={row.get('entry_count')} "
                            f"max_error={row.get('max_error')} "
                            f"radix_bits={row.get('radix_bits')} "
                            f"radix_buckets={row.get('radix_buckets')} "
                            f"artifact={row.get('artifact')}"
                        )
                if len(parts) > 25:
                    print(f"  ... ({len(parts)} manifests total)")

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
    if INDEX_FILTER_ALL_COW_SAFE:
        profiles = [p for p in profiles if p["id"] not in COW_UNSAFE_PROFILE_IDS]
    elif INDEX_FILTER is not None:
        profiles = [p for p in profiles if p["id"] in INDEX_FILTER]
        missing = INDEX_FILTER - {p["id"] for p in profiles}
        if missing:
            print(f"WARNING: unknown HUDI_INDEX_FILTER ids ignored: {sorted(missing)}", file=sys.stderr)

    if not profiles:
        print("No profiles to run (check HUDI_INDEX_FILTER).", file=sys.stderr)
        return 2

    print(
        f"Benchmark: {len(profiles)} index profile(s), {ROUNDS} round(s), "
        f"initial={N_INITIAL}, updates={N_UPDATES}, inserts={N_INSERTS}, "
        f"partition_buckets={PARTITION_BUCKET_COUNT}, "
        f"key_distribution={KEY_DISTRIBUTION}, base={BASE_ROOT}"
    )
    if INDEX_FILTER_ALL_COW_SAFE:
        print("  (HUDI_INDEX_FILTER empty or ALL: COW-safe set; excluded: BUCKET_CONSISTENT_HASHING)")
    for p in profiles:
        print(f"  - {p['id']}: {p['label']}")

    print_radix_configuration(active_spark(), profiles)

    all_by_profile: Dict[str, List[Dict[str, Any]]] = {p["id"]: [] for p in profiles}

    for p in profiles:
        for r in range(1, ROUNDS + 1):
            m = run_one_round(p, r, profiles)
            all_by_profile[p["id"]].append(m)
            status = "OK" if m["ok"] else "FAIL"
            print(f"[{status}] {p['id']} round {r}: {m}")
        if CLEANUP_AFTER_PROFILE:
            cleanup_profile_round_paths(p["id"])
            print(f"[CLEANUP] deleted round paths for profile {p['id']}")

    summaries: List[Dict[str, Any]] = []
    for p in profiles:
        runs = all_by_profile[p["id"]]
        last_run = runs[-1]
        write_seconds = [
            last_run.get("bulk_insert_seconds"),
            last_run.get("upsert_existing_seconds"),
            last_run.get("insert_new_seconds"),
            last_run.get("mixed_upsert_seconds"),
        ]
        validate_seconds = [
            last_run.get("snapshot_count_seconds"),
            last_run.get("updated_payload_check_seconds"),
            last_run.get("new_payload_check_seconds"),
            last_run.get("mixed_existing_payload_check_seconds"),
            last_run.get("mixed_new_payload_check_seconds"),
        ]
        write_total_seconds = round(sum(x for x in write_seconds if isinstance(x, (int, float))), 2)
        validate_total_seconds = round(sum(x for x in validate_seconds if isinstance(x, (int, float))), 2)
        summary: Dict[str, Any] = {
            "profile_id": p["id"],
            "profile_label": p["label"],
            "rounds": ROUNDS,
            "base_root": BASE_ROOT,
            "table_name": TABLE_NAME,
            "n_initial": N_INITIAL,
            "n_updates": N_UPDATES,
            "n_inserts": N_INSERTS,
            "partition_buckets": PARTITION_BUCKET_COUNT,
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
            "last_bulk_insert_seconds": last_run.get("bulk_insert_seconds"),
            "last_upsert_existing_seconds": last_run.get("upsert_existing_seconds"),
            "last_insert_new_seconds": last_run.get("insert_new_seconds"),
            "last_mixed_upsert_seconds": last_run.get("mixed_upsert_seconds"),
            "last_snapshot_count_seconds": last_run.get("snapshot_count_seconds"),
            "last_updated_payload_check_seconds": last_run.get("updated_payload_check_seconds"),
            "last_new_payload_check_seconds": last_run.get("new_payload_check_seconds"),
            "last_mixed_existing_payload_check_seconds": last_run.get("mixed_existing_payload_check_seconds"),
            "last_mixed_new_payload_check_seconds": last_run.get("mixed_new_payload_check_seconds"),
            "last_total_write_seconds": write_total_seconds,
            "last_total_validation_seconds": validate_total_seconds,
            "last_total_round_seconds": round(write_total_seconds + validate_total_seconds, 2),
            "last_final_count": last_run.get("final_count"),
            "last_updated_payload_rows": last_run.get("updated_payload_rows"),
            "last_new_payload_rows": last_run.get("new_payload_rows"),
            "last_mixed_existing_payload_rows": last_run.get("mixed_existing_payload_rows"),
            "last_mixed_new_payload_rows": last_run.get("mixed_new_payload_rows"),
        }
        if p["id"] == "RADIX_SPLINE":
            rs = last_run.get("radix_artifact_stats") or {}
            summ = rs.get("summary") or {}
            summary["radix_total_spline_points"] = summ.get("total_spline_points_created") or summ.get(
                "spline_points_sum"
            )
            summary["radix_model_partitions"] = summ.get("partitions_with_valid_spline_header")
            summary["radix_effective_params"] = last_run.get("radix_effective_params")
        summaries.append(summary)

    print("\n=== SUMMARY (per index) ===")
    for s in summaries:
        print(s)

    print_comparison_table(summaries)

    json_path = os.environ.get("HUDI_BENCH_JSON_SUMMARY", "").strip()
    if json_path:
        payload = {
            "n_initial": N_INITIAL,
            "n_updates": N_UPDATES,
            "n_inserts": N_INSERTS,
            "partition_buckets": PARTITION_BUCKET_COUNT,
            "key_distribution": KEY_DISTRIBUTION,
            "rounds": ROUNDS,
            "base_root": BASE_ROOT,
            "table_name": TABLE_NAME,
            "summaries": summaries,
            "runs_by_profile": all_by_profile,
        }
        json_abs = os.path.abspath(json_path)
        dirpath = os.path.dirname(json_abs)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(json_abs, "w", encoding="utf-8") as jf:
            json.dump(payload, jf, indent=2, default=str)
        print(f"\nWrote JSON summary to {json_abs}")

    print(
        "\nNote: RECORD_INDEX enum is deprecated; use GLOBAL_RECORD_LEVEL_INDEX "
        "(same Spark engine class). FLINK_STATE is not applicable to Spark."
    )

    return 0 if all(s["all_rounds_ok"] for s in summaries) else 1


if __name__ == "__main__":
    raise SystemExit(main())
