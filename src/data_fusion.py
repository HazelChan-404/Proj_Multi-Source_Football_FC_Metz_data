"""
Data Fusion - vue player_fused (SB + SC + TM).
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


def build_player_fused(conn=None):
    """
    Build or refresh the player_fused table.
    Aggregates event stats, physical/tracking metrics, and Transfermarkt context
    into a single player-level view.
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()

    # Clear and rebuild (idempotent)
    cursor.execute(f"DELETE FROM {table('player_fused')}")
    conn.commit()

    # Get all players
    cursor.execute(f"""
        SELECT player_id, player_name,
               market_value, market_value_numeric, contract_expiry, current_club
        FROM {table('players')}
    """)
    players = cursor.fetchall()

    count = 0
    for row in players:
        player_id, player_name, mv, mv_num, contract, club = row

        # Event stats (best season / latest)
        cursor.execute(f"""
            SELECT minutes_played, goals_90, assists_90, np_xg_90, shots_90,
                   passes_90, tackles_90, pressures_90, obv_90
            FROM {table('player_season_stats')}
            WHERE player_id = %s
            ORDER BY season_id DESC
            LIMIT 1
        """, (player_id,))
        event_row = cursor.fetchone()

        if event_row:
            (mp_sb, g90, a90, xg90, s90, p90, t90, pr90, obv) = event_row
            has_event = 1
        else:
            mp_sb = g90 = a90 = xg90 = s90 = p90 = t90 = pr90 = obv = None
            has_event = 0

        # Physical/tracking aggregates
        cursor.execute(f"""
            SELECT COUNT(*), AVG(total_distance_m), AVG(sprinting_distance_m),
                   AVG(max_speed_kmh), AVG(num_sprints), AVG(num_high_speed_runs)
            FROM {table('player_match_physical')}
            WHERE player_id = %s
        """, (player_id,))
        phys_row = cursor.fetchone()

        if phys_row and phys_row[0] and phys_row[0] > 0:
            matches_tracked = phys_row[0]
            avg_dist = phys_row[1]
            avg_sprint = phys_row[2]
            avg_speed = phys_row[3]
            avg_sprints = phys_row[4]
            avg_hsr = phys_row[5]
            has_tracking = 1
        else:
            matches_tracked = avg_dist = avg_sprint = avg_speed = avg_sprints = avg_hsr = None
            has_tracking = 0

        # Context from players table (Transfermarkt)
        has_context = 1 if (mv or mv_num or contract or club) else 0

        sources_linked = has_event + has_tracking + has_context

        cursor.execute(f"""
            INSERT INTO {table('player_fused')}
            (player_id, player_name,
             minutes_played_sb, goals_90, assists_90, np_xg_90, shots_90,
             passes_90, tackles_90, pressures_90, obv_90,
             matches_tracked, avg_total_distance_m, avg_sprinting_m,
             avg_max_speed_kmh, avg_sprints, avg_high_speed_runs,
             market_value, market_value_numeric, contract_expiry, current_club,
             has_event_data, has_tracking_data, has_context_data, sources_linked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s)
        """, (
            player_id, player_name,
            mp_sb, g90, a90, xg90, s90, p90, t90, pr90, obv,
            matches_tracked, avg_dist, avg_sprint, avg_speed, avg_sprints, avg_hsr,
            mv, mv_num, contract, club,
            bool(has_event), bool(has_tracking), bool(has_context), sources_linked,
        ))
        count += 1

    conn.commit()
    print(f"Built player_fused: {count} players")

    # Stats
    cursor.execute(
        f"SELECT COUNT(*) FROM {table('player_fused')} WHERE sources_linked >= 2"
    )
    multi = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table('player_fused')} WHERE sources_linked = 3")
    all_three = cursor.fetchone()[0]
    print(f"   With 2+ sources: {multi}, With all 3: {all_three}")

    return count


if __name__ == "__main__":
    conn = get_connection()
    from src.database import create_schema
    create_schema(conn)
    build_player_fused(conn)
    conn.close()
