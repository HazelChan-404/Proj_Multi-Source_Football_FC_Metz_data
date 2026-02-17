"""
Backfill player_match_physical depuis raw_physical_json.
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


def get_metric(record, *keys):
    for key in keys:
        val = record.get(key)
        if val is not None:
            return val
    return None


def main():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT physical_id, raw_physical_json FROM {table('player_match_physical')}"
    )
    rows = cur.fetchall()
    updated = 0

    for physical_id, raw_json in rows:
        if not raw_json:
            continue
        try:
            record = json.loads(raw_json)
        except Exception:
            continue

        minutes = get_metric(
            record, "minutes_full_all", "minutes_played", "minutes"
        )
        total_dist = get_metric(
            record,
            "total_distance_full_all",
            "total_distance",
            "distance_total",
        )
        running = get_metric(
            record,
            "running_distance_full_all",
            "running_distance",
            "distance_running",
        )
        hsr = get_metric(
            record,
            "hsr_distance_full_all",
            "high_speed_running_distance",
            "hsr_distance",
        )
        sprint_dist = get_metric(
            record,
            "sprint_distance_full_all",
            "sprinting_distance",
            "sprint_distance",
        )
        sprint_count = get_metric(
            record,
            "sprint_count_full_all",
            "sprint_count",
            "num_sprints",
        )
        hsr_count = get_metric(
            record,
            "hsr_count_full_all",
            "high_speed_run_count",
        )

        cur.execute(
            f"""
            UPDATE {table('player_match_physical')} SET
                minutes_played = COALESCE(%s, minutes_played),
                total_distance_m = COALESCE(%s, total_distance_m),
                running_distance_m = COALESCE(%s, running_distance_m),
                high_speed_running_m = COALESCE(%s, high_speed_running_m),
                sprinting_distance_m = COALESCE(%s, sprinting_distance_m),
                num_sprints = COALESCE(%s, num_sprints),
                num_high_speed_runs = COALESCE(%s, num_high_speed_runs)
            WHERE physical_id = %s
            """,
            (
                minutes,
                total_dist,
                running,
                hsr,
                sprint_dist,
                sprint_count,
                hsr_count,
                physical_id,
            ),
        )
        if cur.rowcount:
            updated += 1

    conn.commit()
    conn.close()
    print(f"Backfilled {updated}/{len(rows)} physical records")


if __name__ == "__main__":
    main()
