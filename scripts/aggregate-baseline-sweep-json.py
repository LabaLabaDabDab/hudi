#!/usr/bin/env python3
#
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
"""Merge HUDI_BENCH_JSON_SUMMARY files from baseline_benchmark scale sweeps into Markdown tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def load(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def md_table(headers: List[str], rows: List[List[str]]) -> str:
    w = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            w[i] = max(w[i], len(c))
    sep = "|" + "|".join("-" * (x + 2) for x in w) + "|"
    head = "|" + "|".join(f" {h:{w[i]}} " for i, h in enumerate(headers)) + "|"
    body = "\n".join(
        "|" + "|".join(f" {c:{w[i]}} " for i, c in enumerate(r)) + "|" for r in rows
    )
    return "\n".join([head, sep, body])


def mixed_rps(s: Dict[str, Any]) -> Optional[float]:
    v = s.get("median_mixed_upsert_rows_per_sec")
    if v is None:
        return None
    return float(v)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("json_files", nargs="+", type=Path, help="n_*.json from HUDI_BENCH_JSON_SUMMARY")
    ap.add_argument("--radix-only", action="store_true", help="print only RADIX_SPLINE slice")
    args = ap.parse_args()

    runs: List[Tuple[int, Dict[str, Any]]] = []
    for p in args.json_files:
        if not p.exists():
            print(f"missing: {p}", file=sys.stderr)
            continue
        data = load(p)
        n0 = int(data.get("n_initial", 0))
        runs.append((n0, data))

    runs.sort(key=lambda x: x[0])
    if not runs:
        print("no valid inputs", file=sys.stderr)
        return 2

    lines: List[str] = []
    lines.append("# Baseline sweep aggregate\n")
    lines.append("Mixed upsert throughput (median rows/sec) and snapshot time; higher RPS is better.\n")

    table_rows: List[List[str]] = []
    radix_rows: List[List[str]] = []

    for n_initial, data in runs:
        summaries: List[Dict[str, Any]] = data.get("summaries") or []
        by_id = {s["profile_id"]: s for s in summaries}

        rad = by_id.get("RADIX_SPLINE")
        rad_mix = mixed_rps(rad) if rad else None
        rad_snap = rad.get("median_snapshot_count_seconds") if rad else None
        rad_pts = rad.get("radix_total_spline_points") if rad else None
        rad_ok = rad.get("all_rounds_ok") if rad else None

        ranked: List[Tuple[str, float]] = []
        for s in summaries:
            pid = s["profile_id"]
            m = mixed_rps(s)
            if m is not None:
                ranked.append((pid, m))
        ranked.sort(key=lambda x: -x[1])
        rank_map = {pid: i + 1 for i, (pid, _) in enumerate(ranked)}
        radix_rank = rank_map.get("RADIX_SPLINE", "-")

        best = ranked[0][0] if ranked else "-"
        gap = ""
        if rad_mix is not None and ranked:
            topv = ranked[0][1]
            if topv and topv > 0:
                pct = 100.0 * rad_mix / topv
                gap = " (best)" if best == "RADIX_SPLINE" else f" ({pct:.1f}% of best)"

        table_rows.append(
            [
                str(n_initial),
                f"{rad_mix:.2f}" if rad_mix is not None else "-",
                str(radix_rank),
                best,
                f"{rad_snap:.2f}" if rad_snap is not None else "-",
                str(rad_pts) if rad_pts is not None else "-",
                "ok" if rad_ok else "fail" if rad is not None else "-",
            ]
        )

        radix_rows.append(
            [
                str(n_initial),
                f"{rad_mix:.2f}" if rad_mix is not None else "-",
                str(radix_rank),
                f"{rad_snap:.2f}" if rad_snap is not None else "-",
                str(rad_pts) if rad_pts is not None else "-",
                best + (gap if gap else ""),
            ]
        )

    if not args.radix_only:
        lines.append(
            md_table(
                [
                    "N_initial",
                    "RADIX mixed RPS",
                    "rank (mixed)",
                    "best (mixed)",
                    "RADIX snap_s",
                    "radix spline pts",
                    "RADIX ok",
                ],
                table_rows,
            )
        )
        lines.append("\n")

    lines.append("## RADIX_SPLINE vs best profile (mixed upsert)\n")
    lines.append(
        md_table(
            ["N_initial", "RADIX mixed RPS", "rank", "snap_s", "spline pts", "context"],
            radix_rows,
        )
    )
    lines.append("\n")

    lines.append(
        "Interpretation hints: see `scripts/baseline-sweep-radix-interpretation.md` in this repo.\n"
    )

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
