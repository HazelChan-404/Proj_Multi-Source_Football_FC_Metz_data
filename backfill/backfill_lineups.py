"""
Backfill match_lineups pour les matchs sans lineup.
为已入库的 StatsBomb 比赛补全 lineup 数据（首发阵容/出场名单）

Usage:
  python backfill/backfill_lineups.py          # 补全所有
  python backfill/backfill_lineups.py --limit N  # 仅补全前 N 场
"""

import sys
import os

# Ajouter le répertoire racine au path pour les imports
# 将项目根目录加入 sys.path，以便导入 config 和 src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import STATSBOMB_CREDS
from src.database import get_connection, table
from src.statsbomb_ingestion import find_ligue1_current_season, get_competitions
from statsbombpy import sb


def main():
    # --- Arguments en ligne de commande ---
    # 命令行参数：--limit N 限制处理场数
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help='Max matches to process')
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Obtenir la saison Ligue 1 actuelle et tous les matchs de StatsBomb
    # 获取 Ligue 1 当前赛季，并从 StatsBomb API 拉取该赛季所有比赛
    comp_id, season_id, _ = find_ligue1_current_season(get_competitions())
    matches = sb.matches(competition_id=comp_id, season_id=season_id, creds=STATSBOMB_CREDS)
    match_ids = matches['match_id'].tolist()

    # Identifier les matchs qui n'ont pas encore de lineup dans la base
    # 找出 DB 中已有但 match_lineups 表里没有记录的比赛
    to_fetch = []
    for sb_mid in match_ids:
        # Vérifier si ce match existe dans notre table matches
        # ① 检查该比赛是否在 matches 表中
        cur.execute(
            f"SELECT match_id FROM {table('matches')} WHERE statsbomb_match_id = %s",
            (int(sb_mid),)
        )
        row = cur.fetchone()
        if not row:
            continue  # Match non trouvé en DB → ignorer / 不在 DB 中则跳过

        # Vérifier si lineup existe déjà pour ce match
        # ② 检查 match_lineups 中是否已有该 match 的 lineup
        cur.execute(
            f"SELECT 1 FROM {table('match_lineups')} WHERE match_id = %s LIMIT 1",
            (row[0],)  # row[0] = match_id interne (notre clé primaire)
        )
        if cur.fetchone() is None:
            to_fetch.append(sb_mid)  # Pas de lineup → à récupérer / 没有则加入待补全列表

    if not to_fetch:
        print("All matches already have lineups.")
        conn.close()
        return

    # Limiter le nombre si --limit spécifié
    # 若指定 --limit，只处理前 N 场
    if args.limit:
        to_fetch = to_fetch[:args.limit]
    print(f"Backfilling lineups for {len(to_fetch)} matches...")

    # Appeler l'ingestion des lineups (StatsBomb API + INSERT en DB)
    # 调用 lineup 入库逻辑：拉取 API 并写入 match_lineups
    from src.statsbomb_ingestion import ingest_match_lineups
    matches_df = matches[matches['match_id'].isin(to_fetch)]
    count = ingest_match_lineups(conn, matches_df, max_matches=None)
    print(f"Done: {count} lineup entries inserted.")
    conn.close()


if __name__ == "__main__":
    main()
