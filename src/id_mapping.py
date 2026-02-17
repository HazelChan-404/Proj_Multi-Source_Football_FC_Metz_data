"""
ID Mapping - association SB/SC/TM par similarité de noms.
"""

import re
import sys
import os
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


def normalize_name(name):
    """
    Normalize a player name for fuzzy matching.
    Removes accents, lowercases, removes special characters.
    """
    if not name:
        return ""
    # Remove accents
    nfkd = unicodedata.normalize('NFKD', name)
    name_ascii = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase and remove special characters
    name_clean = re.sub(r'[^a-z\s]', '', name_ascii.lower())
    # Remove extra whitespace
    name_clean = ' '.join(name_clean.split())
    return name_clean


def name_similarity(name1, name2):
    """
    Calculate similarity between two player names.
    Returns a score between 0 and 1.
    """
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    if not n1 or not n2:
        return 0.0

    # Exact match
    if n1 == n2:
        return 1.0

    # Check if one name contains the other
    if n1 in n2 or n2 in n1:
        return 0.85

    # Split into parts and check overlap
    parts1 = set(n1.split())
    parts2 = set(n2.split())

    if not parts1 or not parts2:
        return 0.0

    # Jaccard similarity on name parts
    intersection = parts1 & parts2
    union = parts1 | parts2
    jaccard = len(intersection) / len(union)

    # Bonus for matching last name (usually the most important)
    last1 = n1.split()[-1]
    last2 = n2.split()[-1]
    last_match_bonus = 0.3 if last1 == last2 else 0.0

    return min(1.0, jaccard + last_match_bonus)


def build_player_id_mapping(conn=None):
    """
    Build and populate the player_id_mapping table.
    This reconciles player IDs across all three data sources.
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    print("\n🔗 Building player ID mapping...")

    # Get all players that have at least one source ID
    cursor.execute(f"""
        SELECT player_id, player_name, statsbomb_player_name,
               statsbomb_player_id, skillcorner_player_id, 
               skillcorner_player_name, transfermarkt_player_id
        FROM {table('players')}
    """)
    all_players = cursor.fetchall()

    # Build mapping entries
    count = 0
    for player in all_players:
        pid, pname, sb_name, sb_id, sc_id, sc_name, tm_id = player

        # Only create mapping if at least one source ID exists
        if sb_id or sc_id or tm_id:
            cursor.execute(f"""
                INSERT INTO {table('player_id_mapping')}
                (player_id, statsbomb_player_id, skillcorner_player_id,
                 transfermarkt_player_id, mapping_method, confidence)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    statsbomb_player_id = EXCLUDED.statsbomb_player_id,
                    skillcorner_player_id = EXCLUDED.skillcorner_player_id,
                    transfermarkt_player_id = EXCLUDED.transfermarkt_player_id,
                    mapping_method = EXCLUDED.mapping_method,
                    confidence = EXCLUDED.confidence
            """, (
                pid, sb_id, sc_id, tm_id,
                'auto_build',
                1.0 if (sb_id and sc_id) or (sb_id and tm_id) else 0.8
            ))
            count += 1

    conn.commit()
    print(f"✅ Created {count} player ID mappings")

    # Print mapping statistics
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE statsbomb_player_id IS NOT NULL")
    sb_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE skillcorner_player_id IS NOT NULL")
    sc_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE transfermarkt_player_id IS NOT NULL")
    tm_count = cursor.fetchone()[0]
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table('players')} 
        WHERE statsbomb_player_id IS NOT NULL 
        AND skillcorner_player_id IS NOT NULL
    """)
    sb_sc = cursor.fetchone()[0]
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table('players')} 
        WHERE statsbomb_player_id IS NOT NULL 
        AND transfermarkt_player_id IS NOT NULL
    """)
    sb_tm = cursor.fetchone()[0]
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table('players')} 
        WHERE statsbomb_player_id IS NOT NULL 
        AND skillcorner_player_id IS NOT NULL
        AND transfermarkt_player_id IS NOT NULL
    """)
    all_three = cursor.fetchone()[0]

    print(f"\n📊 Player ID Mapping Statistics:")
    print(f"   Players with StatsBomb ID:     {sb_count}")
    print(f"   Players with SkillCorner ID:   {sc_count}")
    print(f"   Players with Transfermarkt ID: {tm_count}")
    print(f"   Linked SB + SC:                {sb_sc}")
    print(f"   Linked SB + TM:                {sb_tm}")
    print(f"   Linked all three sources:       {all_three}")

    return count


def attempt_fuzzy_matching(conn=None):
    """
    Attempt to link unmatched players using fuzzy name matching.
    Links SB<->SC and also SB<->TM where TM has data but SB player exists with different name.
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    print("\n🔍 Attempting fuzzy name matching for unlinked players...")

    # 1. SB <-> SC merge (merge SC-only into SB when name matches)
    cursor.execute(f"""
        SELECT player_id, player_name, statsbomb_player_name, statsbomb_player_id
        FROM {table('players')}
        WHERE statsbomb_player_id IS NOT NULL
        AND skillcorner_player_id IS NULL
    """)
    sb_only = cursor.fetchall()

    cursor.execute(f"""
        SELECT player_id, player_name, skillcorner_player_name, skillcorner_player_id
        FROM {table('players')}
        WHERE skillcorner_player_id IS NOT NULL
        AND statsbomb_player_id IS NULL
    """)
    sc_only = cursor.fetchall()

    merged = 0
    for sb_player in sb_only:
        sb_pid, sb_pname, sb_sbname, sb_id = sb_player
        search_name = sb_sbname or sb_pname

        best_match = None
        best_score = 0

        for sc_player in sc_only:
            sc_pid, sc_pname, sc_scname, sc_id = sc_player
            target_name = sc_scname or sc_pname

            score = name_similarity(search_name, target_name)
            if score > best_score and score >= 0.7:
                best_score = score
                best_match = sc_player

        if best_match:
            sc_pid, sc_pname, sc_scname, sc_id = best_match

            # Libérer la contrainte unique : vider sc_id sur sc_pid avant de l'assigner à sb_pid
            cursor.execute(
                f"UPDATE {table('players')} SET skillcorner_player_id = NULL, skillcorner_player_name = NULL WHERE player_id = %s",
                (sc_pid,)
            )
            cursor.execute(f"""
                UPDATE {table('players')} SET
                    skillcorner_player_id = %s,
                    skillcorner_player_name = %s,
                    updated_at = NOW()
                WHERE player_id = %s
            """, (sc_id, sc_scname or sc_pname, sb_pid))

            cursor.execute(f"DELETE FROM {table('players')} WHERE player_id = %s", (sc_pid,))

            merged += 1
            sc_only = [p for p in sc_only if p[0] != sc_pid]

    # 2. TM <-> SB: merge TM-only into SB when name matches (TM has market_value etc.)
    cursor.execute(f"""
        SELECT player_id, player_name, transfermarkt_player_id, market_value
        FROM {table('players')}
        WHERE transfermarkt_player_id IS NOT NULL
        AND statsbomb_player_id IS NULL
    """)
    tm_only = cursor.fetchall()

    cursor.execute(f"""
        SELECT player_id, player_name, statsbomb_player_id
        FROM {table('players')}
        WHERE statsbomb_player_id IS NOT NULL
        AND transfermarkt_player_id IS NULL
    """)
    sb_no_tm = cursor.fetchall()

    tm_merged = 0
    for tm_player in tm_only:
        tm_pid, tm_pname, tm_id, tm_mv = tm_player
        best = None
        best_score = 0
        for sb_player in sb_no_tm:
            sb_pid, sb_pname, sb_id = sb_player
            score = name_similarity(tm_pname or "", sb_pname or sb_player[1] or "")
            if score >= 0.75 and score > best_score:
                best_score = score
                best = sb_player

        if best:
            sb_pid, sb_pname, sb_id = best
            # Copy TM fields to SB player (don't delete TM player - FK constraints)
            cursor.execute(
                f"""SELECT transfermarkt_url, market_value, market_value_numeric,
                          contract_expiry, current_club, jersey_number, nationality,
                          height_cm, preferred_foot, date_of_birth, primary_position, agent
                   FROM {table('players')} WHERE player_id = %s""",
                (tm_pid,),
            )
            tm_fields = cursor.fetchone()
            if tm_fields:
                cursor.execute(
                    f"""
                    UPDATE {table('players')} SET
                        transfermarkt_player_id = %s,
                        transfermarkt_url = COALESCE(transfermarkt_url, %s),
                        market_value = COALESCE(market_value, %s),
                        market_value_numeric = COALESCE(market_value_numeric, %s),
                        contract_expiry = COALESCE(contract_expiry, %s),
                        current_club = COALESCE(current_club, %s),
                        jersey_number = COALESCE(jersey_number, %s),
                        nationality = COALESCE(nationality, %s),
                        height_cm = COALESCE(height_cm, %s),
                        preferred_foot = COALESCE(preferred_foot, %s),
                        date_of_birth = COALESCE(date_of_birth, %s),
                        primary_position = COALESCE(primary_position, %s),
                        agent = COALESCE(agent, %s),
                        updated_at = NOW()
                    WHERE player_id = %s
                    """,
                    (tm_id,) + tm_fields + (sb_pid,),
                )
                tm_merged += 1
                sb_no_tm = [p for p in sb_no_tm if p[0] != sb_pid]

    conn.commit()
    print(f"✅ Fuzzy matched and merged {merged} SB<->SC, {tm_merged} TM<->SB player records")
    return merged + tm_merged


def print_database_summary(conn=None):
    """Print a summary of all data in the database."""
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()

    print("\n" + "="*60)
    print("📊 DATABASE SUMMARY")
    print("="*60)

    tables = [
        ("competitions", "Competitions"),
        ("seasons", "Seasons"),
        ("teams", "Teams"),
        ("matches", "Matches"),
        ("players", "Players"),
        ("events", "Events"),
        ("player_season_stats", "Player Season Stats"),
        ("player_match_physical", "Physical Data Records"),
        ("player_id_mapping", "ID Mappings"),
        ("match_lineups", "Lineup Entries"),
        ("player_fused", "Fused Player View"),
    ]

    for tbl_name, label in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table(tbl_name)}")
            count = cursor.fetchone()[0]
            print(f"   {label:.<35} {count:>6}")
        except Exception:
            print(f"   {label:.<35} {'N/A':>6}")

    # Players with multiple source coverage
    print("\n   --- Player Source Coverage ---")
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE statsbomb_player_id IS NOT NULL")
    print(f"   {'With StatsBomb ID':.<35} {cursor.fetchone()[0]:>6}")
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE skillcorner_player_id IS NOT NULL")
    print(f"   {'With SkillCorner ID':.<35} {cursor.fetchone()[0]:>6}")
    cursor.execute(f"SELECT COUNT(*) FROM {table('players')} WHERE transfermarkt_player_id IS NOT NULL")
    print(f"   {'With Transfermarkt ID':.<35} {cursor.fetchone()[0]:>6}")
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table('players')} 
        WHERE statsbomb_player_id IS NOT NULL 
        AND skillcorner_player_id IS NOT NULL
        AND transfermarkt_player_id IS NOT NULL
    """)
    print(f"   {'All 3 sources linked':.<35} {cursor.fetchone()[0]:>6}")

    # Match linking
    print("\n   --- Match Source Coverage ---")
    cursor.execute(f"SELECT COUNT(*) FROM {table('matches')} WHERE statsbomb_match_id IS NOT NULL")
    print(f"   {'With StatsBomb ID':.<35} {cursor.fetchone()[0]:>6}")
    cursor.execute(f"SELECT COUNT(*) FROM {table('matches')} WHERE skillcorner_match_id IS NOT NULL")
    print(f"   {'With SkillCorner ID':.<35} {cursor.fetchone()[0]:>6}")
    cursor.execute(f"""
        SELECT COUNT(*) FROM {table('matches')} 
        WHERE statsbomb_match_id IS NOT NULL 
        AND skillcorner_match_id IS NOT NULL
    """)
    print(f"   {'Both sources linked':.<35} {cursor.fetchone()[0]:>6}")

    print("="*60)


if __name__ == "__main__":
    conn = get_connection()
    build_player_id_mapping(conn)
    attempt_fuzzy_matching(conn)
    print_database_summary(conn)
    conn.close()
