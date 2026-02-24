"""
Backfill skillcorner_team_id avec la nouvelle méthode matching=statsbomb.
用新方法 matching=statsbomb 回填/修正 teams 表的 skillcorner_team_id

Contexte / 适用场景 :
  旧方法按名称模糊匹配可能产生错误的 skillcorner_team_id。
  此脚本用 matching=statsbomb + statsbomb_id 重新建立正确映射并更新 BDD。

Usage: python backfill/backfill_teams_sc_mapping.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SKILLCORNER_USERNAME, SKILLCORNER_PASSWORD
from src.database import get_connection, table
from src.skillcorner_ingestion import find_ligue1_edition, get_client, _team_name_matches


def main():
    print("=" * 60)
    print("Backfill skillcorner_team_id (matching=statsbomb)")
    print("=" * 60)

    client = get_client()

    # 1) matching=statsbomb + édition Ligue 1
    teams_sb_all = client.get_teams(params={'matching': 'statsbomb'})
    sb_lookup = {t['id']: t for t in teams_sb_all}

    edition_id, _ = find_ligue1_edition(client)
    if not edition_id:
        print("Erreur : édition Ligue 1 non trouvée.")
        return

    teams_ligue1 = client.get_teams(params={'competition_edition': edition_id})
    teams = []
    for t in teams_ligue1:
        enriched = sb_lookup.get(t['id'], t)
        teams.append({**t, 'statsbomb_id': enriched.get('statsbomb_id')})

    # 2) Mapping sb_id -> sc_id pour les équipes avec statsbomb_id
    sb_to_sc = {}
    for t in teams:
        sid = t.get('statsbomb_id')
        if sid:
            try:
                sb_to_sc[int(sid)] = t['id']
            except (TypeError, ValueError):
                pass

    print(f"Équipes Ligue 1 avec statsbomb_id: {len(sb_to_sc)}")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT team_id, team_name, statsbomb_team_id, skillcorner_team_id FROM {table('teams')}"
    )
    db_teams = cur.fetchall()

    updated = 0
    for team_id, team_name, sb_id, sc_id_old in db_teams:
        sc_id_new = None

        # Priorité 1 : par statsbomb_team_id
        if sb_id is not None:
            try:
                sb_id = int(sb_id)
                sc_id_new = sb_to_sc.get(sb_id)
            except (TypeError, ValueError):
                pass

        # Priorité 2 : par nom (fallback)
        if sc_id_new is None:
            for t in teams:
                if _team_name_matches(team_name, t.get('name')):
                    sc_id_new = t['id']
                    break

        if sc_id_new is not None and sc_id_new != sc_id_old:
            cur.execute(
                f"UPDATE {table('teams')} SET skillcorner_team_id = %s WHERE team_id = %s",
                (sc_id_new, team_id)
            )
            updated += 1
            print(f"  {team_name}: {sc_id_old} -> {sc_id_new}")

    conn.commit()
    conn.close()

    print(f"\nMise à jour de {updated} équipes.")
    print("Terminé.")


if __name__ == "__main__":
    main()
