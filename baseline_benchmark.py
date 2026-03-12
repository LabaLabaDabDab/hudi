import os
import time
from statistics import median

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

BASE_ROOT = os.environ.get("HUDI_BASE_ROOT", "hdfs://namenode:9000/user/hudi/trips_cow")
TABLE_NAME = os.environ.get("HUDI_TABLE_NAME", "trips_cow")

N_INITIAL = int(os.environ.get("N_INITIAL", "1000000"))
N_UPDATES = int(os.environ.get("N_UPDATES", "100000"))
N_INSERTS = int(os.environ.get("N_INSERTS", "100000"))
ROUNDS = int(os.environ.get("ROUNDS", "3"))

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

common_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "dt",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}


def delete_path_if_exists(path_str: str) -> None:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    if fs.exists(path):
        fs.delete(path, True)


def timed_write(df, operation: str, mode: str, path: str, extra_options: dict | None = None) -> float:
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


def timed_action(label: str, fn):
    t0 = time.perf_counter()
    result = fn()
    t1 = time.perf_counter()
    return {
        "label": label,
        "seconds": round(t1 - t0, 2),
        "result": result,
    }


def build_initial_df():
    return (
        spark.range(0, N_INITIAL)
        .withColumn("ts", expr("id"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('v_', id)"))
    )


def build_scattered_updates_df():
    return (
        spark.range(0, N_UPDATES)
        .withColumn("id", expr(f"pmod(id * 15485863, {N_INITIAL})"))
        .withColumn("ts", expr("id + 100000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('updated_', id)"))
    )


def build_new_inserts_df():
    return (
        spark.range(N_INITIAL, N_INITIAL + N_INSERTS)
        .withColumn("ts", expr("id + 200000000"))
        .withColumn("dt", expr("concat('2026-03-', lpad(cast((id % 10) + 1 as string), 2, '0'))"))
        .withColumn("payload", expr("concat('new_', id)"))
    )


def build_mixed_df():
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


def round_path(round_no: int) -> str:
    return f"{BASE_ROOT.rstrip('/')}_round_{round_no}"


def run_one_round(round_no: int):
    spark.catalog.clearCache()

    path = round_path(round_no)
    delete_path_if_exists(path)

    metrics = {
        "round": round_no,
        "base_path": path,
    }

    initial_df = build_initial_df()
    bulk_insert_seconds = timed_write(initial_df, "bulk_insert", "overwrite", path)
    metrics["bulk_insert_seconds"] = round(bulk_insert_seconds, 2)
    metrics["bulk_insert_rows_per_sec"] = round(N_INITIAL / bulk_insert_seconds, 2)

    updates_df = build_scattered_updates_df()
    upsert_seconds = timed_write(updates_df, "upsert", "append", path)
    metrics["upsert_existing_seconds"] = round(upsert_seconds, 2)
    metrics["upsert_existing_rows_per_sec"] = round(N_UPDATES / upsert_seconds, 2)

    inserts_df = build_new_inserts_df()
    insert_seconds = timed_write(inserts_df, "insert", "append", path)
    metrics["insert_new_seconds"] = round(insert_seconds, 2)
    metrics["insert_new_rows_per_sec"] = round(N_INSERTS / insert_seconds, 2)

    mixed_df = build_mixed_df()
    mixed_seconds = timed_write(mixed_df, "upsert", "append", path)
    metrics["mixed_upsert_seconds"] = round(mixed_seconds, 2)
    metrics["mixed_upsert_rows_per_sec"] = round(N_UPDATES / mixed_seconds, 2)

    result_df = spark.read.format("hudi").load(path)

    count_metric = timed_action("snapshot_count", lambda: result_df.count())
    metrics["snapshot_count_seconds"] = count_metric["seconds"]
    metrics["final_count"] = count_metric["result"]

    updated_check = timed_action(
        "updated_payload_check",
        lambda: result_df.filter("payload like 'updated_%'").count()
    )
    metrics["updated_payload_rows"] = updated_check["result"]
    metrics["updated_payload_check_seconds"] = updated_check["seconds"]

    new_check = timed_action(
        "new_payload_check",
        lambda: result_df.filter("payload like 'new_%'").count()
    )
    metrics["new_payload_rows"] = new_check["result"]
    metrics["new_payload_check_seconds"] = new_check["seconds"]

    mixed_existing_check = timed_action(
        "mixed_existing_payload_check",
        lambda: result_df.filter("payload like 'mixed_existing_%'").count()
    )
    metrics["mixed_existing_payload_rows"] = mixed_existing_check["result"]
    metrics["mixed_existing_payload_check_seconds"] = mixed_existing_check["seconds"]

    mixed_new_check = timed_action(
        "mixed_new_payload_check",
        lambda: result_df.filter("payload like 'mixed_new_%'").count()
    )
    metrics["mixed_new_payload_rows"] = mixed_new_check["result"]
    metrics["mixed_new_payload_check_seconds"] = mixed_new_check["seconds"]

    return metrics


all_metrics = []
for r in range(1, ROUNDS + 1):
    m = run_one_round(r)
    all_metrics.append(m)
    print(f"ROUND {r}: {m}")

summary = {
    "rounds": ROUNDS,
    "base_root": BASE_ROOT,
    "table_name": TABLE_NAME,
    "n_initial": N_INITIAL,
    "n_updates": N_UPDATES,
    "n_inserts": N_INSERTS,
    "median_bulk_insert_rows_per_sec": median(x["bulk_insert_rows_per_sec"] for x in all_metrics),
    "median_upsert_existing_rows_per_sec": median(x["upsert_existing_rows_per_sec"] for x in all_metrics),
    "median_insert_new_rows_per_sec": median(x["insert_new_rows_per_sec"] for x in all_metrics),
    "median_mixed_upsert_rows_per_sec": median(x["mixed_upsert_rows_per_sec"] for x in all_metrics),
    "median_snapshot_count_seconds": median(x["snapshot_count_seconds"] for x in all_metrics),
    "last_final_count": all_metrics[-1]["final_count"],
    "last_updated_payload_rows": all_metrics[-1]["updated_payload_rows"],
    "last_new_payload_rows": all_metrics[-1]["new_payload_rows"],
    "last_mixed_existing_payload_rows": all_metrics[-1]["mixed_existing_payload_rows"],
    "last_mixed_new_payload_rows": all_metrics[-1]["mixed_new_payload_rows"],
}

print("SUMMARY")
print(summary)