"""
Analyse FC Metz — Saison 2025/2026 Ligue 1

Utilise les données : StatsBomb (events, stats), SkillCorner (physical), Transfermarkt (market_value).
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


def get_metz_team_id(cursor):
    """Retourne team_id de Metz."""
    cursor.execute(
        f"SELECT team_id FROM {table('teams')} WHERE LOWER(team_name) LIKE '%metz%'"
    )
    row = cursor.fetchone()
    return row[0] if row else None


def analyse_bilan(conn):
    """Bilan des matchs : V-N-D, buts."""
    cur = conn.cursor()
    metz_id = get_metz_team_id(cur)
    if not metz_id:
        return None

    cur.execute(f"""
        WITH mtz AS (
            SELECT m.match_id, m.home_team_id, m.away_team_id, m.home_score, m.away_score
            FROM {table('matches')} m
            WHERE (m.home_team_id = %s OR m.away_team_id = %s) AND m.season_id = 318
        )
        SELECT 
            SUM(CASE WHEN (home_team_id=%s AND home_score>away_score) OR (away_team_id=%s AND away_score>home_score) THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN home_score=away_score THEN 1 ELSE 0 END) as draws,
            SUM(CASE WHEN (home_team_id=%s AND home_score<away_score) OR (away_team_id=%s AND away_score<home_score) THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN home_team_id=%s THEN home_score ELSE away_score END) as gf,
            SUM(CASE WHEN home_team_id=%s THEN away_score ELSE home_score END) as ga
        FROM mtz
        WHERE home_score IS NOT NULL
    """, (metz_id,) * 8)
    return cur.fetchone()


def analyse_classement(conn):
    """Classement Ligue 1."""
    cur = conn.cursor()
    cur.execute(f"""
        WITH team_pts AS (
            SELECT 
                CASE WHEN m.home_team_id = t.team_id THEN m.home_team_id ELSE m.away_team_id END as tid,
                SUM(CASE 
                    WHEN (m.home_team_id=t.team_id AND m.home_score>m.away_score) OR (m.away_team_id=t.team_id AND m.away_score>m.home_score) THEN 3
                    WHEN m.home_score = m.away_score THEN 1 ELSE 0 
                END) as pts,
                SUM(CASE WHEN m.home_team_id=t.team_id THEN m.home_score ELSE m.away_score END) as gf,
                SUM(CASE WHEN m.home_team_id=t.team_id THEN m.away_score ELSE m.home_score END) as ga
            FROM {table('matches')} m, {table('teams')} t
            WHERE m.season_id = 318 AND (m.home_team_id=t.team_id OR m.away_team_id=t.team_id)
            AND m.home_score IS NOT NULL
            GROUP BY tid
        )
        SELECT t.team_name, tp.pts, tp.gf, tp.ga, (tp.gf - tp.ga) as diff
        FROM team_pts tp JOIN {table('teams')} t ON tp.tid = t.team_id
        ORDER BY tp.pts DESC, (tp.gf-tp.ga) DESC
    """)
    return cur.fetchall()


def analyse_joueurs_cles(conn):
    """Joueurs les plus utilisés et performants."""
    cur = conn.cursor()
    metz_id = get_metz_team_id(cur)
    if not metz_id:
        return []

    cur.execute(f"""
        SELECT pf.player_name, pf.minutes_played_sb, pf.goals_90, pf.assists_90, 
               pf.np_xg_90, pf.shots_90, pf.passes_90, pf.tackles_90, pf.pressures_90,
               pf.obv_90, pf.avg_total_distance_m, pf.avg_sprinting_m, pf.market_value
        FROM {table('player_fused')} pf
        JOIN {table('match_lineups')} ml ON pf.player_id = ml.player_id
        JOIN {table('matches')} m ON ml.match_id = m.match_id
        WHERE ml.team_id = %s AND m.season_id = 318
        GROUP BY pf.player_id, pf.player_name, pf.minutes_played_sb, pf.goals_90, pf.assists_90,
                 pf.np_xg_90, pf.shots_90, pf.passes_90, pf.tackles_90, pf.pressures_90,
                 pf.obv_90, pf.avg_total_distance_m, pf.avg_sprinting_m, pf.market_value
        ORDER BY pf.minutes_played_sb DESC NULLS LAST
        LIMIT 25
    """, (metz_id,))
    return cur.fetchall()


def analyse_matchs_recent(conn, n=10):
    """Derniers matchs."""
    cur = conn.cursor()
    metz_id = get_metz_team_id(cur)
    if not metz_id:
        return []

    cur.execute(f"""
        SELECT m.match_date, h.team_name as home, a.team_name as away,
               m.home_score, m.away_score
        FROM {table('matches')} m
        JOIN {table('teams')} h ON m.home_team_id = h.team_id
        JOIN {table('teams')} a ON m.away_team_id = a.team_id
        WHERE (m.home_team_id = %s OR m.away_team_id = %s) AND m.season_id = 318
        ORDER BY m.match_date DESC
        LIMIT %s
    """, (metz_id, metz_id, n))
    return cur.fetchall()


def main():
    conn = get_connection()
    cur = conn.cursor()

    metz_id = get_metz_team_id(cur)
    if not metz_id:
        print("⚠️  Équipe Metz non trouvée dans la base.")
        conn.close()
        return

    print("=" * 60)
    print("📊 FC METZ — ANALYSE SAISON 2025/2026 LIGUE 1")
    print("=" * 60)

    # Bilan
    bilan = analyse_bilan(conn)
    if bilan:
        w, d, l, gf, ga = bilan
        print(f"\n📈 Bilan : {w}V - {d}N - {l}D  |  Buts : {gf} marqués, {ga} encaissés")
        print(f"   Points : {w*3 + d}  |  Différence : {gf - ga}")

    # Classement
    classement = analyse_classement(conn)
    metz_rank = next((i+1 for i, r in enumerate(classement) if 'Metz' in r[0]), None)
    print(f"\n🏆 Classement : {metz_rank}e / {len(classement)}")
    if metz_rank:
        m = [r for r in classement if 'Metz' in r[0]][0]
        print(f"   Metz : {m[1]} pts, {m[2]} buts, {m[3]} encaissés, diff {m[4]}")

    # Joueurs
    players = analyse_joueurs_cles(conn)
    print(f"\n👥 Effectif : {len(players)} joueurs avec des minutes")
    print("   Top 5 (minutes) :")
    for p in players[:5]:
        name = (p[0] or "")[:25]
        mp = p[1] or 0
        g90 = p[2] or 0
        mv = p[12] if len(p) > 12 else "-"  # market_value
        print(f"      {name:<25} {mp:>5} min | goals_90: {g90:.2f} | {mv or '-'}")

    # Derniers matchs
    matches = analyse_matchs_recent(conn, 5)
    print("\n📅 5 derniers matchs :")
    for r in matches:
        print(f"   {r[0]} | {r[1]} {r[3] or '-'}-{r[4] or '-'} {r[2]}")

    conn.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
