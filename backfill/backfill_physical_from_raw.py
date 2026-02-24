"""
Backfill player_match_physical depuis raw_physical_json.
从 raw_physical_json 解析并填充体能数据的数值列（距离、速度、冲刺等）

Contexte / 适用场景 :
  SkillCorner API 曾变更字段名（如 minutes_full_all, total_distance_full_all）。
  旧记录只存了 raw_physical_json，数值列为空。此脚本解析 JSON 并补全。

Usage: python backfill/backfill_physical_from_raw.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


def get_metric(record, *keys):
    """
    Prend la première clé présente dans le record (API peut varier les noms).
    从 record 中按 keys 顺序取第一个非空值（兼容 API 字段名变化）
    """
    for key in keys:
        val = record.get(key)
        if val is not None:
            return val
    return None


def main():
    conn = get_connection()
    cur = conn.cursor()

    # Récupérer tous les enregistrements avec raw_physical_json
    # 取出所有有 raw_physical_json 的体能记录
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

        # Extraire les métriques depuis le JSON (plusieurs noms possibles selon version API)
        # 从 JSON 中提取指标（兼容多种字段名：SkillCorner 新旧版）
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
        max_speed = get_metric(
            record,
            "top_speed", "max_speed", "peak_speed", "peak_velocity",
            "max_speed_kmh", "top_speed_kmh",
        )
        avg_speed = get_metric(
            record,
            "average_speed", "avg_speed", "avg_speed_kmh",
            "mean_speed", "mean_velocity",
        )
        num_accel = get_metric(
            record,
            "acceleration_count", "num_accelerations", "accelerations",
            "acceleration_count_full_all", "num_accelerations_full_all",
        )
        num_decel = get_metric(
            record,
            "deceleration_count", "num_decelerations", "decelerations",
            "deceleration_count_full_all", "num_decelerations_full_all",
        )

        # Mettre à jour les colonnes numériques (COALESCE = ne pas écraser si déjà rempli)
        cur.execute(
            f"""
            UPDATE {table('player_match_physical')} SET
                minutes_played = COALESCE(%s, minutes_played),
                total_distance_m = COALESCE(%s, total_distance_m),
                running_distance_m = COALESCE(%s, running_distance_m),
                high_speed_running_m = COALESCE(%s, high_speed_running_m),
                sprinting_distance_m = COALESCE(%s, sprinting_distance_m),
                num_sprints = COALESCE(%s, num_sprints),
                num_high_speed_runs = COALESCE(%s, num_high_speed_runs),
                max_speed_kmh = COALESCE(%s, max_speed_kmh),
                avg_speed_kmh = COALESCE(%s, avg_speed_kmh),
                num_accelerations = COALESCE(%s, num_accelerations),
                num_decelerations = COALESCE(%s, num_decelerations)
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
                max_speed,
                avg_speed,
                num_accel,
                num_decel,
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
