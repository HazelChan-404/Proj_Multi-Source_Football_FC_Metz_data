"""
SkillCorner ingestion - teams, matches, players, physical.
"""

import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SKILLCORNER_USERNAME, SKILLCORNER_PASSWORD
from src.database import get_connection, table


def get_client():
    """Create and return a SkillCorner client."""
    from skillcorner.client import SkillcornerClient
    client = SkillcornerClient(
        username=SKILLCORNER_USERNAME,
        password=SKILLCORNER_PASSWORD
    )
    return client


# ============================================================
# 1. DISCOVER COMPETITIONS, SEASONS & EDITIONS
# ============================================================

def get_seasons(client):
    """Fetch all available seasons from SkillCorner."""
    print("📡 Fetching SkillCorner seasons...")
    seasons = client.get_seasons()
    print(f"   Found {len(seasons)} seasons")
    return seasons


def get_competitions(client, season_id=None):
    """Fetch competitions, optionally filtered by season."""
    print("📡 Fetching SkillCorner competitions...")
    params = {}
    if season_id:
        params['season'] = season_id
    competitions = client.get_competitions(params=params)
    print(f"   Found {len(competitions)} competitions")
    return competitions


def find_ligue1_edition(client):
    """
    Find the current Ligue 1 competition edition.
    Returns (competition_edition_id, season_info) or (None, None).
    """
    print("📡 Searching for Ligue 1 competition edition...")

    # Get all seasons and find the most recent one
    seasons = get_seasons(client)

    # Sort by ID (higher = more recent)
    seasons_sorted = sorted(seasons, key=lambda s: s.get('id', 0), reverse=True)

    for season in seasons_sorted[:3]:  # Check last 3 seasons
        season_id = season.get('id')
        season_name = season.get('name', '')
        print(f"   Checking season: {season_name} (id={season_id})")

        try:
            competitions = get_competitions(client, season_id=season_id)
        except Exception as e:
            print(f"   ⚠️  Error fetching competitions for season {season_id}: {e}")
            continue

        for comp in competitions:
            comp_name = comp.get('name', '')
            comp_area = comp.get('area', {}).get('name', '') if isinstance(comp.get('area'), dict) else ''

            if 'ligue 1' in comp_name.lower() or (
                'france' in comp_area.lower() and 'ligue' in comp_name.lower()
            ):
                comp_id = comp.get('id')
                print(f"   Found competition: {comp_name} (id={comp_id})")

                # Get competition editions
                try:
                    editions = client.get_competition_editions(
                        params={'season': season_id}
                    )
                    # Also try competition-specific editions
                    if not editions:
                        editions = client.get_competition_competition_editions(
                            competition_id=str(comp_id)
                        )
                except Exception:
                    editions = []

                for edition in editions:
                    ed_id = edition.get('id')
                    ed_name = edition.get('name', '') or ''
                    ed_comp = edition.get('competition', {})
                    ed_comp_name = ed_comp.get('name', '') if isinstance(ed_comp, dict) else ''

                    if 'ligue 1' in ed_name.lower() or 'ligue 1' in ed_comp_name.lower():
                        print(f"   🎯 Found edition: {ed_name} (id={ed_id})")
                        return ed_id, season

                # If no edition found from filtering, try listing all editions for the competition
                try:
                    all_editions = client.get_competition_competition_editions(
                        competition_id=str(comp_id)
                    )
                    if all_editions:
                        # Get the most recent edition
                        latest_edition = sorted(
                            all_editions,
                            key=lambda e: e.get('id', 0),
                            reverse=True
                        )[0]
                        ed_id = latest_edition.get('id')
                        print(f"   🎯 Found latest edition: id={ed_id}")
                        return ed_id, season
                except Exception:
                    pass

    print("    Could not find Ligue 1 edition. Trying with all competition editions...")

    # Fallback: list all competition editions
    try:
        all_editions = client.get_competition_editions()
        for edition in all_editions:
            ed_name = edition.get('name', '') or ''
            ed_comp = edition.get('competition', {})
            ed_comp_name = ed_comp.get('name', '') if isinstance(ed_comp, dict) else ''
            if 'ligue 1' in ed_name.lower() or 'ligue 1' in ed_comp_name.lower():
                ed_id = edition.get('id')
                print(f"   🎯 Found edition (fallback): {ed_name} (id={ed_id})")
                return ed_id, None
    except Exception as e:
        print(f"   ⚠️  Fallback failed: {e}")

    return None, None


# ============================================================
# 2. INGEST TEAMS FROM SKILLCORNER
# ============================================================

def ingest_teams(conn, client, competition_edition_id):
    """Fetch and store SkillCorner teams, linking to existing teams by name."""
    print(f"📡 Fetching SkillCorner teams for edition {competition_edition_id}...")

    try:
        teams = client.get_teams(params={'competition_edition': competition_edition_id})
    except Exception as e:
        print(f"    Error fetching teams: {e}")
        return []

    cursor = conn.cursor()
    count = 0

    for team in teams:
        sc_team_id = team.get('id')
        team_name = team.get('name', '')
        short_name = team.get('short_name', '')

        # Try to match with existing team by name (fuzzy match)
        cursor.execute(
            f"SELECT team_id, team_name FROM {table('teams')} WHERE "
            "LOWER(team_name) LIKE %s OR LOWER(team_name) LIKE %s",
            (f"%{team_name.lower()}%", f"%{short_name.lower()}%")
        )
        existing = cursor.fetchone()

        if existing:
            # Update existing team with SkillCorner ID
            cursor.execute(
                f"UPDATE {table('teams')} SET skillcorner_team_id = %s WHERE team_id = %s",
                (sc_team_id, existing[0])
            )
            count += 1
        else:
            # Insert new team
            cursor.execute(f"""
                INSERT INTO {table('teams')} (team_name, skillcorner_team_id)
                VALUES (%s, %s)
                ON CONFLICT (team_name) DO UPDATE SET skillcorner_team_id = EXCLUDED.skillcorner_team_id
            """, (team_name, sc_team_id))
            count += 1

    conn.commit()
    print(f" Processed {count} teams from SkillCorner")
    return teams


# ============================================================
# 3. INGEST MATCHES FROM SKILLCORNER
# ============================================================

def ingest_matches(conn, client, competition_edition_id):
    """Fetch and link SkillCorner matches to existing StatsBomb matches."""
    print(f"📡 Fetching SkillCorner matches for edition {competition_edition_id}...")

    try:
        matches = client.get_matches(params={'competition_edition': competition_edition_id})
    except Exception as e:
        print(f"   ⚠️  Error fetching matches: {e}")
        return []

    cursor = conn.cursor()
    linked = 0

    for match in matches:
        sc_match_id = match.get('id')
        match_date = match.get('date_time', '')[:10] if match.get('date_time') else ''

        home_team = match.get('home_team', {})
        away_team = match.get('away_team', {})
        home_name = home_team.get('name', '') if isinstance(home_team, dict) else ''
        away_name = away_team.get('name', '') if isinstance(away_team, dict) else ''

        # Skip if this SkillCorner match_id is already linked (prevents unique violation)
        cursor.execute(
            f"SELECT 1 FROM {table('matches')} WHERE skillcorner_match_id = %s LIMIT 1",
            (sc_match_id,)
        )
        if cursor.fetchone():
            continue

        # Try to link to existing match by date and team names
        cursor.execute(f"""
            SELECT m.match_id FROM {table('matches')} m
            JOIN {table('teams')} h ON m.home_team_id = h.team_id
            JOIN {table('teams')} a ON m.away_team_id = a.team_id
            WHERE m.match_date = %s
            AND (LOWER(h.team_name) LIKE %s OR LOWER(h.team_name) LIKE %s)
        """, (
            match_date,
            f"%{home_name.lower().split()[0]}%" if home_name else '%',
            f"%{home_name.lower()}%"
        ))
        existing = cursor.fetchone()

        if existing:
            # Only update if target match does not yet have a SkillCorner link
            cursor.execute(
                f"""UPDATE {table('matches')} SET skillcorner_match_id = %s
                    WHERE match_id = %s AND skillcorner_match_id IS NULL""",
                (sc_match_id, existing[0])
            )
            if cursor.rowcount > 0:
                linked += 1
        else:
            # Insert as new match if it can't be linked
            # Try to find team IDs
            home_team_id = None
            away_team_id = None
            if home_name:
                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE LOWER(team_name) LIKE %s",
                    (f"%{home_name.lower()}%",)
                )
                ht = cursor.fetchone()
                if ht:
                    home_team_id = ht[0]

            if away_name:
                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE LOWER(team_name) LIKE %s",
                    (f"%{away_name.lower()}%",)
                )
                at = cursor.fetchone()
                if at:
                    away_team_id = at[0]

            home_score = match.get('home_team_score')
            away_score = match.get('away_team_score')

            cursor.execute(f"""
                INSERT INTO {table('matches')} 
                (skillcorner_match_id, match_date, home_team_id, away_team_id,
                 home_score, away_score)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (skillcorner_match_id) DO NOTHING
            """, (sc_match_id, match_date, home_team_id, away_team_id,
                  home_score, away_score))

    conn.commit()
    print(f"✅ Linked {linked} matches with StatsBomb, processed {len(matches)} total")
    return matches


# ============================================================
# 4. INGEST PLAYERS FROM SKILLCORNER
# ============================================================

def ingest_players(conn, client, competition_edition_id):
    """Fetch SkillCorner players and link to existing players by name."""
    print("📡 Fetching SkillCorner players...")

    cursor = conn.cursor()

    # Get all teams for this competition
    cursor.execute(f"SELECT team_name, skillcorner_team_id FROM {table('teams')} WHERE skillcorner_team_id IS NOT NULL")
    sc_teams = cursor.fetchall()

    count = 0
    for team_name, sc_team_id in sc_teams:
        try:
            # Search for players by team
            players = client.get_players(params={'team': sc_team_id})
            time.sleep(0.3)  # Rate limiting
        except Exception as e:
            print(f"   ⚠️  Error fetching players for team {team_name}: {e}")
            continue

        for player in players:
            sc_player_id = player.get("id")
            player_name = player.get("name", "") or player.get("short_name", "")
            first_name = player.get("first_name", "")
            last_name = player.get("last_name", "")
            full_name = (
                f"{first_name} {last_name}".strip()
                if first_name or last_name
                else player_name
            )
            short_name = player.get("short_name", "") or last_name or player_name

            if not player_name and not full_name:
                continue

            # Skip if this SkillCorner player_id is already linked (prevents unique violation)
            cursor.execute(
                f"SELECT 1 FROM {table('players')} WHERE skillcorner_player_id = %s LIMIT 1",
                (sc_player_id,)
            )
            if cursor.fetchone():
                continue

            # Try multiple name variants for matching
            search_names = [
                full_name or player_name,
                short_name,
                last_name,
                player_name,
            ]
            search_names = [n for n in search_names if n]

            existing = None
            for search_name in search_names:
                cursor.execute(
                    f"""
                SELECT player_id FROM {table('players')}
                WHERE LOWER(player_name) = LOWER(%s)
                OR LOWER(statsbomb_player_name) = LOWER(%s)
                OR LOWER(skillcorner_player_name) = LOWER(%s)
                OR player_name LIKE %s
                OR statsbomb_player_name LIKE %s
            """,
                    (
                        search_name,
                        search_name,
                        search_name,
                        f"%{search_name.split()[-1] if search_name else ''}%",
                        f"%{search_name.split()[-1] if search_name else ''}%",
                    ),
                )
                existing = cursor.fetchone()
                if existing:
                    break

            display_name = full_name or player_name

            if existing:
                # Update existing player with SkillCorner info
                cursor.execute(
                    f"""
                    UPDATE {table('players')} SET
                        skillcorner_player_id = %s,
                        skillcorner_player_name = %s,
                        updated_at = NOW()
                    WHERE player_id = %s AND skillcorner_player_id IS NULL
                    """,
                    (sc_player_id, display_name, existing[0]),
                )
                count += 1
            else:
                # Insert new player
                cursor.execute(
                    f"""
                    INSERT INTO {table('players')} 
                    (skillcorner_player_id, skillcorner_player_name, player_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (skillcorner_player_id) DO NOTHING
                    """,
                    (sc_player_id, display_name, display_name),
                )
                count += 1

    conn.commit()
    print(f"✅ Processed {count} players from SkillCorner")
    return count


# ============================================================
# 5. INGEST PHYSICAL DATA FROM SKILLCORNER
# ============================================================

def ingest_physical_data(conn, client, competition_edition_id):
    """
    Fetch physical/tracking data per player per match from SkillCorner.
    This is the core SkillCorner data: speed, distance, sprints, etc.
    """
    print("📡 Fetching SkillCorner physical data...")

    cursor = conn.cursor()

    # Get all teams with SkillCorner IDs
    cursor.execute(
        f"SELECT team_id, team_name, skillcorner_team_id FROM {table('teams')} "
        "WHERE skillcorner_team_id IS NOT NULL"
    )
    sc_teams = cursor.fetchall()

    total_records = 0

    for team_id, team_name, sc_team_id in sc_teams:
        print(f"   📊 Fetching physical data for {team_name}...")

        try:
            physical_data = client.get_physical(
                params={'team': sc_team_id}
            )
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"   ⚠️  Error fetching physical data for {team_name}: {e}")
            continue

        if not physical_data:
            continue

        for record in physical_data:
            sc_match_id = record.get('match_id') or record.get('match', {}).get('id')
            sc_player_id = record.get('player_id') or record.get('player', {}).get('id')

            if not sc_match_id or not sc_player_id:
                continue

            # Find internal match_id
            internal_match_id = None
            cursor.execute(
                f"SELECT match_id FROM {table('matches')} WHERE skillcorner_match_id = %s",
                (sc_match_id,)
            )
            match_result = cursor.fetchone()
            if match_result:
                internal_match_id = match_result[0]

            # Find internal player_id
            internal_player_id = None
            cursor.execute(
                f"SELECT player_id FROM {table('players')} WHERE skillcorner_player_id = %s",
                (sc_player_id,),
            )
            player_result = cursor.fetchone()
            if player_result:
                internal_player_id = player_result[0]
            else:
                # Try match by name from physical record (backfill SC link)
                sc_player_name = (
                    record.get("player_name")
                    or record.get("player_short_name")
                    or (record.get("player", {}) or {}).get("name")
                )
                if sc_player_name:
                    for name_var in [
                        sc_player_name.strip(),
                        sc_player_name.split()[-1] if sc_player_name else None,
                    ]:
                        if not name_var:
                            continue
                        cursor.execute(
                            f"""
                            SELECT player_id FROM {table('players')}
                            WHERE (skillcorner_player_id IS NULL)
                            AND (
                                LOWER(player_name) = LOWER(%s)
                                OR LOWER(statsbomb_player_name) = LOWER(%s)
                                OR player_name LIKE %s
                            )
                            """,
                            (name_var, name_var, f"%{name_var}%"),
                        )
                        match = cursor.fetchone()
                        if match:
                            # Also fill date_of_birth, primary_position if null
                            dob = record.get("player_birthdate")
                            pos = record.get("position") or record.get("position_group")
                            cursor.execute(
                                f"""
                                UPDATE {table('players')} SET
                                    skillcorner_player_id = %s,
                                    skillcorner_player_name = %s,
                                    date_of_birth = COALESCE(date_of_birth, %s),
                                    primary_position = COALESCE(primary_position, %s),
                                    updated_at = NOW()
                                WHERE player_id = %s AND skillcorner_player_id IS NULL
                                """,
                                (sc_player_id, sc_player_name, dob, pos, match[0]),
                            )
                            internal_player_id = match[0]
                            break

            # Extract physical metrics (SkillCorner API field names - multiple variants)
            def get_metric(record, *keys):
                for key in keys:
                    val = record.get(key)
                    if val is not None:
                        return val
                return None

            raw_json = json.dumps(record, default=str)

            # SkillCorner 2024+ uses *_full_all suffixes (e.g. total_distance_full_all)
            minutes = get_metric(
                record, "minutes_full_all", "minutes_played", "minutes"
            )
            total_dist = get_metric(
                record,
                "total_distance_full_all",
                "total_distance",
                "distance_total",
                "total_distance_m",
            )
            running_dist = get_metric(
                record,
                "running_distance_full_all",
                "running_distance",
                "distance_running",
                "low_speed_running_distance",
            )
            hsr_dist = get_metric(
                record,
                "hsr_distance_full_all",
                "high_speed_running_distance",
                "distance_high_speed_running",
                "hsr_distance",
            )
            sprint_dist = get_metric(
                record,
                "sprint_distance_full_all",
                "sprinting_distance",
                "distance_sprinting",
                "sprint_distance",
            )
            sprint_count = get_metric(
                record,
                "sprint_count_full_all",
                "sprint_count",
                "num_sprints",
                "sprints",
            )
            hsr_count = get_metric(
                record,
                "hsr_count_full_all",
                "high_speed_run_count",
                "num_high_speed_runs",
            )

            try:
                cursor.execute(f"""
                    INSERT INTO {table('player_match_physical')}
                    (match_id, player_id, skillcorner_match_id, skillcorner_player_id,
                     team_name, minutes_played,
                     total_distance_m, walking_distance_m, jogging_distance_m,
                     running_distance_m, high_speed_running_m, sprinting_distance_m,
                     max_speed_kmh, avg_speed_kmh, num_sprints, num_high_speed_runs,
                     num_accelerations, num_decelerations, high_intensity_distance_m,
                     distance_tip_m, distance_otip_m, psv99,
                     raw_physical_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    internal_match_id,
                    internal_player_id,
                    sc_match_id,
                    sc_player_id,
                    team_name,
                    minutes,
                    total_dist,
                    get_metric(record, "walking_distance", "distance_walking"),
                    get_metric(record, "jogging_distance", "distance_jogging"),
                    running_dist,
                    hsr_dist,
                    sprint_dist,
                    get_metric(record, "top_speed", "max_speed", "peak_speed"),
                    get_metric(record, "average_speed", "avg_speed"),
                    sprint_count,
                    hsr_count,
                    get_metric(
                        record,
                        "acceleration_count",
                        "num_accelerations",
                        "accelerations",
                    ),
                    get_metric(
                        record,
                        "deceleration_count",
                        "num_decelerations",
                        "decelerations",
                    ),
                    get_metric(
                        record,
                        "high_intensity_distance",
                        "hi_distance",
                    ),
                    get_metric(
                        record,
                        "distance_tip",
                        "tip_distance",
                        "distance_in_possession",
                    ),
                    get_metric(
                        record,
                        "distance_otip",
                        "otip_distance",
                        "distance_out_of_possession",
                    ),
                    get_metric(record, "psv_99", "psv99"),
                    raw_json,
                ))
                total_records += 1
            except Exception as e:
                continue

        if total_records % 100 == 0:
            conn.commit()

    conn.commit()
    print(f"✅ Stored {total_records} physical data records")
    return total_records


# ============================================================
# MAIN FUNCTION - Run full SkillCorner ingestion
# ============================================================

def run_skillcorner_ingestion(conn=None):
    """
    Run the full SkillCorner data ingestion pipeline.
    
    Args:
        conn: SQLite connection (creates one if None)
    
    Returns:
        competition_edition_id
    """
    if conn is None:
        conn = get_connection()

    print("\n" + "="*60)
    print("🏃 SKILLCORNER DATA INGESTION")
    print("="*60)

    # 1. Create client
    client = get_client()

    # 2. Find Ligue 1 edition
    edition_id, season_info = find_ligue1_edition(client)

    if not edition_id:
        print("❌ Could not find Ligue 1 competition edition!")
        print("   Attempting to use all available data...")
        # Try to get any available data
        try:
            matches = client.get_matches()
            if matches:
                print(f"   Found {len(matches)} matches in total")
                # Look for French matches
                for m in matches[:5]:
                    print(f"   Sample match: {m}")
        except Exception as e:
            print(f"   ⚠️  Error: {e}")
        return None

    print(f"\n   Using competition edition: {edition_id}")

    # 3. Ingest teams
    ingest_teams(conn, client, edition_id)

    # 4. Ingest matches
    ingest_matches(conn, client, edition_id)

    # 5. Ingest players
    ingest_players(conn, client, edition_id)

    # 6. Ingest physical data
    ingest_physical_data(conn, client, edition_id)

    print("\n✅ SkillCorner ingestion complete!")
    return edition_id


if __name__ == "__main__":
    conn = get_connection()
    from src.database import create_schema
    create_schema(conn)
    run_skillcorner_ingestion(conn)
    conn.close()
