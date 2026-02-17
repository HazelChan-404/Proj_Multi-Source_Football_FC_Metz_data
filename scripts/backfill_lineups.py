"""
Backfill match_lineups pour les matchs sans lineup.
Usage: python scripts/backfill_lineups.py [--limit N]
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import STATSBOMB_CREDS
from src.database import get_connection, table
from src.statsbomb_ingestion import find_ligue1_current_season, get_competitions
from statsbombpy import sb

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help='Max matches to process')
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    comp_id, season_id, _ = find_ligue1_current_season(get_competitions())
    matches = sb.matches(competition_id=comp_id, season_id=season_id, creds=STATSBOMB_CREDS)
    match_ids = matches['match_id'].tolist()

    to_fetch = []
    for sb_mid in match_ids:
        cur.execute(
            f"SELECT match_id FROM {table('matches')} WHERE statsbomb_match_id = %s",
            (int(sb_mid),)
        )
        row = cur.fetchone()
        if not row:
            continue
        cur.execute(
            f"SELECT 1 FROM {table('match_lineups')} WHERE match_id = %s LIMIT 1",
            (row[0],)
        )
        if cur.fetchone() is None:
            to_fetch.append(sb_mid)

    if not to_fetch:
        print("All matches already have lineups.")
        conn.close()
        return

    if args.limit:
        to_fetch = to_fetch[:args.limit]
    print(f"Backfilling lineups for {len(to_fetch)} matches...")

    from src.statsbomb_ingestion import ingest_match_lineups
    matches_df = matches[matches['match_id'].isin(to_fetch)]
    count = ingest_match_lineups(conn, matches_df, max_matches=None)
    print(f"Done: {count} lineup entries inserted.")
    conn.close()

if __name__ == "__main__":
    main()
