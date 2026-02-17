"""
Database - PostgreSQL schema et connexion.
"""

import sys
import os
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import POSTGRES_CONFIG, DB_SCHEMA


def table(name):
    """Return schema-qualified table name for use in SQL."""
    return f"{DB_SCHEMA}.{name}"


def get_connection():
    """Get a connection to PostgreSQL."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = False
    return conn


def create_schema(conn=None):
    """
    Create schema and all tables in PostgreSQL.
    """
    if conn is None:
        conn = get_connection()
        should_close = True
    else:
        should_close = False

    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};")
    conn.commit()

    # 1. COMPETITIONS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("competitions")} (
            competition_id      INTEGER PRIMARY KEY,
            competition_name    TEXT NOT NULL,
            country_name        TEXT,
            competition_gender  TEXT,
            competition_youth   TEXT,
            competition_international TEXT,
            source              TEXT DEFAULT 'statsbomb'
        )
    """)

    # 2. SEASONS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("seasons")} (
            season_id           INTEGER PRIMARY KEY,
            season_name         TEXT NOT NULL,
            competition_id      INTEGER REFERENCES {table("competitions")}(competition_id)
        )
    """)

    # 3. TEAMS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("teams")} (
            team_id             SERIAL PRIMARY KEY,
            team_name           TEXT NOT NULL UNIQUE,
            statsbomb_team_id   INTEGER,
            skillcorner_team_id INTEGER,
            transfermarkt_team_id TEXT,
            country             TEXT,
            gender              TEXT
        )
    """)

    # 4. MATCHES
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("matches")} (
            match_id            SERIAL PRIMARY KEY,
            statsbomb_match_id  INTEGER UNIQUE,
            skillcorner_match_id INTEGER UNIQUE,
            competition_id      INTEGER REFERENCES {table("competitions")}(competition_id),
            season_id           INTEGER REFERENCES {table("seasons")}(season_id),
            match_date          TEXT,
            kick_off            TEXT,
            home_team_id        INTEGER REFERENCES {table("teams")}(team_id),
            away_team_id        INTEGER REFERENCES {table("teams")}(team_id),
            home_score          INTEGER,
            away_score          INTEGER,
            stadium             TEXT,
            referee             TEXT,
            match_week          INTEGER,
            match_status        TEXT
        )
    """)

    # 5. PLAYERS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("players")} (
            player_id               SERIAL PRIMARY KEY,
            statsbomb_player_id      INTEGER UNIQUE,
            statsbomb_player_name    TEXT,
            skillcorner_player_id    INTEGER UNIQUE,
            skillcorner_player_name  TEXT,
            transfermarkt_player_id  TEXT,
            transfermarkt_url        TEXT,
            player_name              TEXT NOT NULL,
            player_nickname          TEXT,
            date_of_birth            TEXT,
            nationality              TEXT,
            height_cm                REAL,
            weight_kg                REAL,
            preferred_foot           TEXT,
            primary_position         TEXT,
            secondary_position       TEXT,
            market_value             TEXT,
            market_value_numeric     REAL,
            contract_expiry          TEXT,
            current_club             TEXT,
            jersey_number            TEXT,
            agent                   TEXT,
            created_at               TIMESTAMP DEFAULT NOW(),
            updated_at               TIMESTAMP DEFAULT NOW()
        )
    """)

    # 6. PLAYER_ID_MAPPING
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("player_id_mapping")} (
            mapping_id              SERIAL PRIMARY KEY,
            player_id               INTEGER NOT NULL UNIQUE REFERENCES {table("players")}(player_id),
            statsbomb_player_id     INTEGER,
            skillcorner_player_id   INTEGER,
            transfermarkt_player_id TEXT,
            mapping_method          TEXT,
            confidence              REAL DEFAULT 1.0
        )
    """)

    # 7. MATCH_LINEUPS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("match_lineups")} (
            lineup_id           SERIAL PRIMARY KEY,
            match_id            INTEGER NOT NULL REFERENCES {table("matches")}(match_id),
            player_id           INTEGER NOT NULL REFERENCES {table("players")}(player_id),
            team_id             INTEGER REFERENCES {table("teams")}(team_id),
            jersey_number       INTEGER,
            position            TEXT,
            is_starter          BOOLEAN DEFAULT FALSE,
            minutes_played      INTEGER,
            UNIQUE(match_id, player_id)
        )
    """)

    # 8. EVENTS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("events")} (
            event_id            TEXT PRIMARY KEY,
            match_id             INTEGER NOT NULL REFERENCES {table("matches")}(match_id),
            index_num            INTEGER,
            period               INTEGER,
            timestamp            TEXT,
            minute               INTEGER,
            second               INTEGER,
            event_type           TEXT,
            event_type_id        INTEGER,
            possession           INTEGER,
            possession_team_id   INTEGER,
            play_pattern         TEXT,
            team_id              INTEGER,
            player_id            INTEGER,
            position             TEXT,
            location_x           REAL,
            location_y           REAL,
            duration             REAL,
            under_pressure       BOOLEAN,
            pass_recipient_id    INTEGER,
            pass_length          REAL,
            pass_angle           REAL,
            pass_height          TEXT,
            pass_end_location_x  REAL,
            pass_end_location_y  REAL,
            pass_outcome         TEXT,
            pass_type            TEXT,
            pass_body_part       TEXT,
            pass_cross           BOOLEAN,
            shot_statsbomb_xg    REAL,
            shot_end_location_x  REAL,
            shot_end_location_y  REAL,
            shot_outcome         TEXT,
            shot_technique        TEXT,
            shot_body_part       TEXT,
            shot_type            TEXT,
            shot_first_time      BOOLEAN,
            carry_end_location_x REAL,
            carry_end_location_y REAL,
            dribble_outcome      TEXT,
            obv_total_net        REAL,
            obv_for_net          REAL,
            obv_against_net      REAL
        )
    """)

    # 9. PLAYER_SEASON_STATS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("player_season_stats")} (
            stat_id                     SERIAL PRIMARY KEY,
            player_id                   INTEGER NOT NULL REFERENCES {table("players")}(player_id),
            statsbomb_player_id         INTEGER,
            team_id                     INTEGER,
            competition_id              INTEGER,
            season_id                   INTEGER,
            minutes_played              INTEGER,
            nineties_played             REAL,
            appearances                 INTEGER,
            starting_appearances        INTEGER,
            goals_90                    REAL,
            np_goals_90                 REAL,
            np_xg_90                    REAL,
            np_xg_per_shot              REAL,
            shots_90                    REAL,
            shot_on_target_ratio        REAL,
            conversion_ratio            REAL,
            assists_90                  REAL,
            xa_90                       REAL,
            key_passes_90               REAL,
            passing_ratio               REAL,
            passes_90                   REAL,
            long_balls_90               REAL,
            long_ball_ratio             REAL,
            crosses_90                  REAL,
            crossing_ratio              REAL,
            passes_into_box_90          REAL,
            through_balls_90            REAL,
            deep_progressions_90        REAL,
            dribbles_90                 REAL,
            dribble_ratio               REAL,
            carries_90                  REAL,
            carry_length                REAL,
            turnovers_90                REAL,
            dispossessions_90           REAL,
            tackles_90                  REAL,
            interceptions_90            REAL,
            tackles_and_interceptions_90 REAL,
            clearances_90               REAL,
            blocks_per_shot             REAL,
            pressures_90                REAL,
            counterpressures_90         REAL,
            dribbled_past_90            REAL,
            fouls_90                    REAL,
            obv_90                      REAL,
            obv_pass_90                 REAL,
            obv_shot_90                 REAL,
            obv_defensive_action_90     REAL,
            obv_dribble_carry_90        REAL,
            save_ratio                  REAL,
            goals_faced_90              REAL,
            gsaa_90                     REAL,
            raw_stats_json              TEXT
        )
    """)

    # 10. PLAYER_MATCH_PHYSICAL
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("player_match_physical")} (
            physical_id             SERIAL PRIMARY KEY,
            match_id                INTEGER REFERENCES {table("matches")}(match_id),
            player_id               INTEGER REFERENCES {table("players")}(player_id),
            skillcorner_match_id    INTEGER,
            skillcorner_player_id   INTEGER,
            team_name               TEXT,
            minutes_played          REAL,
            total_distance_m        REAL,
            walking_distance_m      REAL,
            jogging_distance_m      REAL,
            running_distance_m      REAL,
            high_speed_running_m    REAL,
            sprinting_distance_m    REAL,
            max_speed_kmh           REAL,
            avg_speed_kmh           REAL,
            num_sprints             INTEGER,
            num_high_speed_runs     INTEGER,
            num_accelerations       INTEGER,
            num_decelerations       INTEGER,
            high_intensity_distance_m REAL,
            distance_tip_m          REAL,
            distance_otip_m         REAL,
            psv99                   REAL,
            raw_physical_json       TEXT
        )
    """)

    # 11. PLAYER_FUSED
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table("player_fused")} (
            player_id               INTEGER PRIMARY KEY REFERENCES {table("players")}(player_id),
            player_name             TEXT,
            minutes_played_sb       INTEGER,
            goals_90                REAL,
            assists_90              REAL,
            np_xg_90                REAL,
            shots_90                REAL,
            passes_90               REAL,
            tackles_90              REAL,
            pressures_90            REAL,
            obv_90                  REAL,
            matches_tracked         INTEGER,
            avg_total_distance_m    REAL,
            avg_sprinting_m         REAL,
            avg_max_speed_kmh       REAL,
            avg_sprints             REAL,
            avg_high_speed_runs     REAL,
            market_value            TEXT,
            market_value_numeric    REAL,
            contract_expiry         TEXT,
            current_club            TEXT,
            has_event_data          BOOLEAN DEFAULT FALSE,
            has_tracking_data       BOOLEAN DEFAULT FALSE,
            has_context_data        BOOLEAN DEFAULT FALSE,
            sources_linked          INTEGER DEFAULT 0
        )
    """)

    # Indexes
    for idx, tbl, col in [
        ("idx_events_match", "events", "match_id"),
        ("idx_events_player", "events", "player_id"),
        ("idx_events_type", "events", "event_type"),
        ("idx_physical_match", "player_match_physical", "match_id"),
        ("idx_physical_player", "player_match_physical", "player_id"),
        ("idx_lineups_match", "match_lineups", "match_id"),
        ("idx_lineups_player", "match_lineups", "player_id"),
        ("idx_players_sb", "players", "statsbomb_player_id"),
        ("idx_players_sc", "players", "skillcorner_player_id"),
        ("idx_players_tm", "players", "transfermarkt_player_id"),
        ("idx_matches_sb", "matches", "statsbomb_match_id"),
        ("idx_matches_sc", "matches", "skillcorner_match_id"),
    ]:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS {idx} ON {table(tbl)} ("{col}")'
        )

    conn.commit()
    print("✅ Database schema created successfully!")

    if should_close:
        conn.close()


def reset_database():
    """Drop et recrée le schema. Toutes les données sont perdues."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"DROP SCHEMA IF EXISTS {DB_SCHEMA} CASCADE;")
    conn.commit()
    conn.close()
    print(f"🗑️  Dropped schema {DB_SCHEMA}")
    conn = get_connection()
    create_schema(conn)
    conn.close()
    print(f"✅ Database reset and recreated: schema {DB_SCHEMA}")


if __name__ == "__main__":
    create_schema()
    print(f"Database ready: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}, schema {DB_SCHEMA}")
