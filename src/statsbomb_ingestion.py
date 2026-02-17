"""
StatsBomb ingestion - competitions, matches, events, lineups, player_season_stats.
StatsBomb 入库模块 - 联赛、比赛、事件、首发、球员赛季统计

Vue d'ensemble / 功能概述 :
  - 通过 StatsBomb API 拉取 Ligue 1 数据（config 中配置联赛名/国家）
  - competitions, seasons → teams, matches → events, match_lineups, player_season_stats
  - events：传球、射门、带球等逐条事件
  - lineups：首发阵容、换人、出场时间

Flux / 执行顺序 :
  get_competitions → find_ligue1_current_season → ingest_competitions → ingest_matches
  → ingest_events → ingest_match_lineups → ingest_player_season_stats → update_player_info_from_mapping
"""

import json
import sys
import os
import numpy as np
import pandas as pd
from statsbombpy import sb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import STATSBOMB_CREDS, STATSBOMB_COMPETITION_NAME, STATSBOMB_COUNTRY, INCREMENTAL_UPDATE
from src.database import get_connection, table


def _extract_name_or_id(obj):
    """Extract name from API object (can be str or dict with name key)."""
    if obj is None or (isinstance(obj, float) and pd.isna(obj)):
        return None
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("name") or obj.get("id")
    return str(obj)


def _extract_id(obj):
    """Extract id from API object (can be int or dict with id key)."""
    if obj is None or (isinstance(obj, float) and pd.isna(obj)):
        return None
    if isinstance(obj, (int, float)):
        return int(obj)
    if isinstance(obj, dict):
        return obj.get("id")
    return None


# ============================================================
# 1. Découverte compétition / saison
# 1. 发现联赛与赛季（根据 config 筛选 Ligue 1）
# ============================================================

def get_competitions():
    """Fetch all available competitions from StatsBomb."""
    print("📡 Fetching StatsBomb competitions...")
    comps = sb.competitions(creds=STATSBOMB_CREDS)
    print(f"   Found {len(comps)} competition-season combinations")
    return comps


def find_ligue1_current_season(comps_df=None):
    """Retourne (competition_id, season_id, season_name) pour la saison Ligue 1 actuelle."""
    if comps_df is None:
        comps_df = get_competitions()

    comp_name = STATSBOMB_COMPETITION_NAME or "Ligue 1"
    country = STATSBOMB_COUNTRY or "France"

    ligue1 = comps_df[
        (comps_df['competition_name'].str.contains(comp_name, case=False, na=False)) &
        (comps_df['country_name'].str.contains(country, case=False, na=False))
    ]

    if ligue1.empty:
        print("⚠️  Ligue 1 not found. Available competitions:")
        print(comps_df[['competition_name', 'country_name', 'season_name']].to_string())
        # Return the first available competition as fallback
        row = comps_df.iloc[0]
        return int(row['competition_id']), int(row['season_id']), row['season_name']

    # Get the most recent season (highest season_id or by name)
    ligue1_sorted = ligue1.sort_values('season_id', ascending=False)
    latest = ligue1_sorted.iloc[0]

    comp_id = int(latest['competition_id'])
    season_id = int(latest['season_id'])
    season_name = latest['season_name']

    print(f"   🎯 Found: {latest['competition_name']} - {season_name}")
    print(f"      competition_id={comp_id}, season_id={season_id}")

    return comp_id, season_id, season_name


# ============================================================
# 2. Ingestion competitions et seasons
# 2. 联赛与赛季入库（competitions, seasons 表）
# ============================================================

def ingest_competitions(conn, comps_df=None):
    """Store competitions and seasons in the database."""
    if comps_df is None:
        comps_df = get_competitions()

    cursor = conn.cursor()

    for _, row in comps_df.iterrows():
        # Insert competition
        cursor.execute(f"""
            INSERT INTO {table("competitions")} 
            (competition_id, competition_name, country_name, competition_gender,
             competition_youth, competition_international, source)
            VALUES (%s, %s, %s, %s, %s, %s, 'statsbomb')
            ON CONFLICT (competition_id) DO NOTHING
        """, (
            int(row['competition_id']),
            row['competition_name'],
            row.get('country_name', ''),
            row.get('competition_gender', ''),
            str(row.get('competition_youth', False)),
            str(row.get('competition_international', False)),
        ))

        # Insert season
        cursor.execute(f"""
            INSERT INTO {table("seasons")} (season_id, season_name, competition_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (season_id) DO NOTHING
        """, (
            int(row['season_id']),
            row['season_name'],
            int(row['competition_id'])
        ))

    conn.commit()
    print(f"✅ Stored {len(comps_df)} competition-season entries")
    return comps_df


# ============================================================
# 3. Ingestion des matchs
# 3. 比赛入库（matches, teams 表）
# ============================================================

def get_matches(competition_id, season_id):
    """Fetch all matches for a competition-season."""
    print(f"📡 Fetching matches for competition={competition_id}, season={season_id}...")
    matches = sb.matches(
        competition_id=competition_id,
        season_id=season_id,
        creds=STATSBOMB_CREDS
    )
    print(f"   Found {len(matches)} matches")
    return matches


def ingest_matches(conn, competition_id, season_id):
    """Fetch and store matches."""
    matches_df = get_matches(competition_id, season_id)
    cursor = conn.cursor()

    for _, row in matches_df.iterrows():
        sb_match_id = int(row['match_id'])

        # API returns home_team/away_team as object {id, name} or flat string; statsbombpy may flatten
        home_team_name = _extract_name_or_id(row.get('home_team')) or str(row.get('home_team', '') or '')
        home_team_sb_id = _extract_id(row.get('home_team'))
        if home_team_sb_id is None and 'home_team_id' in row and pd.notna(row.get('home_team_id')):
            try:
                home_team_sb_id = int(row['home_team_id'])
            except (TypeError, ValueError):
                pass
        htc = row.get('home_team_country')
        home_team_country = _extract_name_or_id(htc) if isinstance(htc, dict) else str(htc or '')
        home_team_gender = row.get('home_team_gender', '')

        cursor.execute(f"""
            INSERT INTO {table("teams")} (team_name, statsbomb_team_id, country, gender)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (team_name) DO NOTHING
        """, (
            home_team_name,
            home_team_sb_id,
            home_team_country or '',
            home_team_gender or ''
        ))

        # Get home team internal id
        cursor.execute(f"SELECT team_id FROM {table('teams')} WHERE team_name = %s", (home_team_name,))
        home_team_result = cursor.fetchone()
        home_team_id = home_team_result[0] if home_team_result else None

        if home_team_id and home_team_sb_id:
            cursor.execute(f"""
                UPDATE {table("teams")} SET statsbomb_team_id = %s WHERE team_id = %s AND statsbomb_team_id IS NULL
            """, (home_team_sb_id, home_team_id))

        away_team_name = _extract_name_or_id(row.get('away_team')) or str(row.get('away_team', '') or '')
        away_team_sb_id = _extract_id(row.get('away_team'))
        if away_team_sb_id is None and 'away_team_id' in row and pd.notna(row.get('away_team_id')):
            try:
                away_team_sb_id = int(row['away_team_id'])
            except (TypeError, ValueError):
                pass
        atc = row.get('away_team_country')
        away_team_country = _extract_name_or_id(atc) if isinstance(atc, dict) else str(atc or '')
        away_team_gender = row.get('away_team_gender', '')

        cursor.execute(f"""
            INSERT INTO {table("teams")} (team_name, statsbomb_team_id, country, gender)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (team_name) DO NOTHING
        """, (
            away_team_name,
            away_team_sb_id,
            away_team_country or '',
            away_team_gender or ''
        ))

        cursor.execute(f"SELECT team_id FROM {table('teams')} WHERE team_name = %s", (away_team_name,))
        away_team_result = cursor.fetchone()
        away_team_id = away_team_result[0] if away_team_result else None

        if away_team_id and away_team_sb_id:
            cursor.execute(f"""
                UPDATE {table("teams")} SET statsbomb_team_id = %s WHERE team_id = %s AND statsbomb_team_id IS NULL
            """, (away_team_sb_id, away_team_id))

        stadium_name = _extract_name_or_id(row.get('stadium'))
        referee_name = _extract_name_or_id(row.get('referee')) or _extract_name_or_id(row.get('referee_name'))

        # Insert match
        cursor.execute(f"""
            INSERT INTO {table("matches")} 
            (statsbomb_match_id, competition_id, season_id, match_date, kick_off,
             home_team_id, away_team_id, home_score, away_score, stadium, referee,
             match_week, match_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (statsbomb_match_id) DO NOTHING
        """, (
            sb_match_id,
            competition_id,
            season_id,
            str(row.get('match_date', '')),
            str(row.get('kick_off', '')),
            home_team_id,
            away_team_id,
            int(row['home_score']) if pd.notna(row.get('home_score')) else None,
            int(row['away_score']) if pd.notna(row.get('away_score')) else None,
            stadium_name,
            referee_name,
            int(row['match_week']) if pd.notna(row.get('match_week')) else None,
            row.get('match_status', '')
        ))

    conn.commit()
    print(f"✅ Stored {len(matches_df)} matches")
    return matches_df


# ============================================================
# 4. Ingestion des events
# 4. 事件入库（events 表：传球、射门、带球等）
# ============================================================

def get_events(match_id):
    """Fetch all events for a single match. Returns empty DataFrame if no events (API returns empty)."""
    try:
        events = sb.events(match_id=match_id, creds=STATSBOMB_CREDS)
        return events if events is not None and not (hasattr(events, 'empty') and events.empty) else pd.DataFrame()
    except (ValueError, Exception) as e:
        # statsbombpy raises ValueError("No objects to concatenate") when match has no events
        if "No objects to concatenate" in str(e) or "No objects" in str(e):
            return pd.DataFrame()
        raise


def _has_val(val):
    """Safe check for 'value exists' - avoids 'truth value of array' error with numpy arrays."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if isinstance(val, np.ndarray):
        return val.size > 0
    if isinstance(val, (list, tuple)):
        return len(val) > 0
    return True


def _to_pg_bool(val):
    """Convert to Python bool for PostgreSQL BOOLEAN. Returns None if null/NaN."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    if isinstance(val, (int, float)):
        return bool(val)
    return None


def _to_pg_val(val):
    """Convert to native Python type for PostgreSQL (psycopg2 can't adapt numpy.int64/float64)."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, (np.floating, np.float64, np.float32)):
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, np.ndarray):
        if val.size == 0:
            return None
        if val.size == 1:
            return _to_pg_val(val.item())
        return None
    return val


def ingest_events(conn, matches_df, max_matches=None):
    """Fetch and store events for all matches."""
    cursor = conn.cursor()
    total_events = 0
    insert_errors_logged = 0

    match_ids = matches_df['match_id'].tolist()
    if max_matches:
        match_ids = match_ids[:max_matches]

    if INCREMENTAL_UPDATE:
        # Skip matches we already have events for
        to_fetch = []
        for sb_mid in match_ids:
            cursor.execute(
                f"SELECT 1 FROM {table('events')} e "
                f"JOIN {table('matches')} m ON e.match_id = m.match_id "
                f"WHERE m.statsbomb_match_id = %s LIMIT 1",
                (int(sb_mid),)
            )
            if cursor.fetchone() is None:
                to_fetch.append(sb_mid)
        match_ids = to_fetch
        if not match_ids:
            print("📡 All matches already have events (INCREMENTAL_UPDATE), skipping.")
            return 0
        print(f"📡 Fetching events for {len(match_ids)} matches (skip {len(matches_df) - len(match_ids)} already ingested)...")
    else:
        print(f"📡 Fetching events for {len(match_ids)} matches...")

    for i, sb_match_id in enumerate(match_ids):
        try:
            events_df = get_events(int(sb_match_id))
            if events_df.empty:
                continue

            # Get internal match_id
            cursor.execute(
                f"SELECT match_id FROM {table('matches')} WHERE statsbomb_match_id = %s",
                (int(sb_match_id),)
            )
            result = cursor.fetchone()
            if not result:
                continue
            internal_match_id = result[0]

            for _, evt in events_df.iterrows():
                # Get player internal id (or insert new player)
                player_internal_id = None
                sb_player_id = None
                player_name = None

                if _has_val(evt.get('player')) and _has_val(evt.get('player_id')):
                    sb_player_id = int(evt['player_id'])
                    player_name = str(evt['player'])

                    # Try to find existing player
                    cursor.execute(
                        f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
                        (sb_player_id,)
                    )
                    player_result = cursor.fetchone()
                    if player_result:
                        player_internal_id = player_result[0]
                    else:
                        # Insert new player
                        cursor.execute(f"""
                            INSERT INTO {table("players")} 
                            (statsbomb_player_id, statsbomb_player_name, player_name)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (statsbomb_player_id) DO NOTHING
                        """, (sb_player_id, player_name, player_name))
                        cursor.execute(
                            f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
                            (sb_player_id,)
                        )
                        player_result = cursor.fetchone()
                        if player_result:
                            player_internal_id = player_result[0]

                # Extract location (handle list/tuple/np.ndarray - avoid pd.notna on arrays)
                location_x, location_y = None, None
                loc = evt.get('location')
                if _has_val(loc):
                    if isinstance(loc, (list, tuple, np.ndarray)) and len(loc) >= 2:
                        location_x, location_y = float(loc[0]), float(loc[1])

                # Extract pass details
                pass_end_x, pass_end_y = None, None
                pel = evt.get('pass_end_location')
                if _has_val(pel):
                    if isinstance(pel, (list, tuple, np.ndarray)) and len(pel) >= 2:
                        pass_end_x, pass_end_y = float(pel[0]), float(pel[1])

                pass_recipient_id = None
                if _has_val(evt.get('pass_recipient_id')):
                    pass_recipient_id = int(evt['pass_recipient_id'])

                # Extract shot details
                shot_end_x, shot_end_y = None, None
                sel = evt.get('shot_end_location')
                if _has_val(sel):
                    if isinstance(sel, (list, tuple, np.ndarray)) and len(sel) >= 2:
                        shot_end_x, shot_end_y = float(sel[0]), float(sel[1])

                # Extract carry details
                carry_end_x, carry_end_y = None, None
                cel = evt.get('carry_end_location')
                if _has_val(cel):
                    if isinstance(cel, (list, tuple, np.ndarray)) and len(cel) >= 2:
                        carry_end_x, carry_end_y = float(cel[0]), float(cel[1])

                # Get team_id
                team_internal_id = None
                if _has_val(evt.get('team')):
                    cursor.execute(
                        f"SELECT team_id FROM {table('teams')} WHERE team_name = %s",
                        (str(evt['team']),)
                    )
                    team_result = cursor.fetchone()
                    if team_result:
                        team_internal_id = team_result[0]

                # Get possession team id
                poss_team_id = None
                if _has_val(evt.get('possession_team')):
                    cursor.execute(
                        f"SELECT team_id FROM {table('teams')} WHERE team_name = %s",
                        (str(evt['possession_team']),)
                    )
                    poss_result = cursor.fetchone()
                    if poss_result:
                        poss_team_id = poss_result[0]

                # Safe get for optional fields (avoids array/Series in boolean context)
                def safe_get(col, default=None):
                    val = evt.get(col)
                    if val is None:
                        return default
                    if isinstance(val, float) and pd.isna(val):
                        return default
                    if isinstance(val, np.ndarray):
                        if val.size == 0:
                            return default
                        return val.item() if val.size == 1 else default
                    return val

                # Event id must be valid (StatsBomb UUID string)
                event_id_val = evt.get('id')
                if not _has_val(event_id_val):
                    continue
                event_id_str = str(event_id_val)

                try:
                    cursor.execute(f"""
                        INSERT INTO {table("events")} 
                        (event_id, match_id, index_num, period, timestamp, minute, second,
                         event_type, event_type_id, possession, possession_team_id,
                         play_pattern, team_id, player_id, position,
                         location_x, location_y, duration, under_pressure,
                         pass_recipient_id, pass_length, pass_angle, pass_height,
                         pass_end_location_x, pass_end_location_y, pass_outcome,
                         pass_type, pass_body_part, pass_cross,
                         shot_statsbomb_xg, shot_end_location_x, shot_end_location_y,
                         shot_outcome, shot_technique, shot_body_part, shot_type,
                         shot_first_time,
                         carry_end_location_x, carry_end_location_y,
                         dribble_outcome,
                         obv_total_net, obv_for_net, obv_against_net)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                    """, (
                        event_id_str,
                        internal_match_id,
                        _to_pg_val(safe_get('index')),
                        _to_pg_val(safe_get('period')),
                        safe_get('timestamp'),  # str
                        _to_pg_val(safe_get('minute')),
                        _to_pg_val(safe_get('second')),
                        safe_get('type'),  # str
                        _to_pg_val(safe_get('type_id')),
                        _to_pg_val(safe_get('possession')),
                        poss_team_id,
                        safe_get('play_pattern'),  # str
                        team_internal_id,
                        player_internal_id,
                        safe_get('position'),  # str
                        _to_pg_val(location_x),
                        _to_pg_val(location_y),
                        _to_pg_val(safe_get('duration')),
                        _to_pg_bool(safe_get('under_pressure')),
                        pass_recipient_id,
                        _to_pg_val(safe_get('pass_length')),
                        _to_pg_val(safe_get('pass_angle')),
                        safe_get('pass_height'),  # str
                        _to_pg_val(pass_end_x),
                        _to_pg_val(pass_end_y),
                        safe_get('pass_outcome'),
                        safe_get('pass_type'),
                        safe_get('pass_body_part'),
                        _to_pg_bool(safe_get('pass_cross')),
                        _to_pg_val(safe_get('shot_statsbomb_xg')),
                        _to_pg_val(shot_end_x),
                        _to_pg_val(shot_end_y),
                        safe_get('shot_outcome'),
                        safe_get('shot_technique'),
                        safe_get('shot_body_part'),
                        safe_get('shot_type'),
                        _to_pg_bool(safe_get('shot_first_time')),
                        _to_pg_val(carry_end_x),
                        _to_pg_val(carry_end_y),
                        safe_get('dribble_outcome'),
                        _to_pg_val(safe_get('obv_total_net')),
                        _to_pg_val(safe_get('obv_for_net')),
                        _to_pg_val(safe_get('obv_against_net')),
                    ))
                except Exception as e:
                    # Log first few failures to help debug; skip individual events that fail
                    if insert_errors_logged < 3:
                        insert_errors_logged += 1
                        print(f"   ⚠️  Event insert error #{insert_errors_logged} (event_id={event_id_str[:24]}...): {e}")
                    continue

                total_events += 1

            if (i + 1) % 5 == 0:
                conn.commit()
                print(f"   Progress: {i+1}/{len(match_ids)} matches processed ({total_events} events)")

        except Exception as e:
            print(f"   ⚠️  Error fetching events for match {sb_match_id}: {e}")
            continue

    conn.commit()
    print(f"✅ Stored {total_events} events from {len(match_ids)} matches")
    return total_events


# ============================================================
# 5. Ingestion des lineups (compositions d'équipe)
# 5. 首发/换人入库（match_lineups 表）
# ============================================================

def _parse_minutes_from_timestamp(ts):
    """Parse 'HH:MM:SS.mmm' or 'HH:MM:SS' to total minutes (int)."""
    if not ts or (isinstance(ts, float) and pd.isna(ts)):
        return None
    ts = str(ts).strip()
    parts = ts.split(':')
    if len(parts) < 3:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
        s = float(parts[2]) if '.' in parts[2] else int(parts[2])
        return int(h * 60 + m + s / 60)
    except (ValueError, TypeError):
        return None


def ingest_match_lineups(conn, matches_df, max_matches=None):
    """
    Fetch and store match lineups from StatsBomb API.
    Populates match_lineups with player, team, position, is_starter, minutes_played.
    """
    cursor = conn.cursor()
    match_ids = matches_df['match_id'].tolist()
    if max_matches:
        match_ids = match_ids[:max_matches]

    if INCREMENTAL_UPDATE:
        to_fetch = []
        for sb_mid in match_ids:
            cursor.execute(
                f"SELECT match_id FROM {table('matches')} WHERE statsbomb_match_id = %s",
                (int(sb_mid),)
            )
            row = cursor.fetchone()
            if not row:
                continue
            internal_match_id = row[0]
            cursor.execute(
                f"SELECT 1 FROM {table('match_lineups')} WHERE match_id = %s LIMIT 1",
                (internal_match_id,)
            )
            if cursor.fetchone() is None:
                to_fetch.append(sb_mid)
        match_ids = to_fetch
        if not match_ids:
            print("📡 All matches already have lineups (INCREMENTAL_UPDATE), skipping.")
            return 0
        print(f"📡 Fetching lineups for {len(match_ids)} matches...")
    else:
        print(f"📡 Fetching lineups for {len(match_ids)} matches...")

    total_lineups = 0
    for i, sb_match_id in enumerate(match_ids):
        try:
            lineups_raw = sb.lineups(match_id=int(sb_match_id), fmt="dict", creds=STATSBOMB_CREDS)
            if not lineups_raw:
                continue

            cursor.execute(
                f"SELECT match_id FROM {table('matches')} WHERE statsbomb_match_id = %s",
                (int(sb_match_id),)
            )
            result = cursor.fetchone()
            if not result:
                continue
            internal_match_id = result[0]

            for sb_team_id, team_data in lineups_raw.items():
                team_name = team_data.get("team_name") or ""
                lineup_list = team_data.get("lineup") or []

                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE statsbomb_team_id = %s OR LOWER(team_name) = LOWER(%s)",
                    (int(sb_team_id), team_name)
                )
                team_row = cursor.fetchone()
                internal_team_id = team_row[0] if team_row else None

                for p in lineup_list:
                    sb_player_id = p.get("player_id")
                    player_name = p.get("player_name") or p.get("player_nickname") or ""
                    jersey_number = p.get("jersey_number")
                    positions = p.get("positions") or []

                    if not sb_player_id:
                        continue

                    position_str = None
                    is_starter = False
                    minutes_played = None

                    if positions:
                        first_pos = positions[0]
                        position_str = first_pos.get("position")
                        is_starter = (first_pos.get("start_reason") or "") == "Starting XI"
                        last_pos = positions[-1]
                        to_ts = last_pos.get("to")
                        if to_ts:
                            minutes_played = _parse_minutes_from_timestamp(to_ts)

                    cursor.execute(
                        f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
                        (int(sb_player_id),)
                    )
                    player_row = cursor.fetchone()
                    if player_row:
                        internal_player_id = player_row[0]
                    else:
                        cursor.execute(f"""
                            INSERT INTO {table("players")}
                            (statsbomb_player_id, statsbomb_player_name, player_name)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (statsbomb_player_id) DO NOTHING
                        """, (int(sb_player_id), player_name, player_name or "Unknown"))
                        cursor.execute(
                            f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
                            (int(sb_player_id),)
                        )
                        player_row = cursor.fetchone()
                        if not player_row:
                            continue
                        internal_player_id = player_row[0]

                    try:
                        cursor.execute(f"""
                            INSERT INTO {table("match_lineups")}
                            (match_id, player_id, team_id, jersey_number, position, is_starter, minutes_played)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_id, player_id) DO UPDATE SET
                                team_id = COALESCE(EXCLUDED.team_id, match_lineups.team_id),
                                jersey_number = COALESCE(EXCLUDED.jersey_number, match_lineups.jersey_number),
                                position = COALESCE(EXCLUDED.position, match_lineups.position),
                                is_starter = COALESCE(EXCLUDED.is_starter, match_lineups.is_starter),
                                minutes_played = COALESCE(EXCLUDED.minutes_played, match_lineups.minutes_played)
                        """, (
                            internal_match_id,
                            internal_player_id,
                            internal_team_id,
                            int(jersey_number) if jersey_number is not None and pd.notna(jersey_number) else None,
                            position_str,
                            is_starter,
                            minutes_played,
                        ))
                        total_lineups += 1
                    except Exception as e:
                        continue

            if (i + 1) % 5 == 0:
                conn.commit()
                print(f"   Lineups progress: {i+1}/{len(match_ids)} matches ({total_lineups} entries)")

        except Exception as e:
            print(f"   ⚠️  Error fetching lineups for match {sb_match_id}: {e}")
            continue

    conn.commit()
    print(f"✅ Stored {total_lineups} lineup entries from {len(match_ids)} matches")
    return total_lineups


# ============================================================
# 6. Ingestion des statistiques joueur par saison
# 6. 球员赛季统计入库（player_season_stats 表：goals_90, xG, passes 等）
# ============================================================

def ingest_player_season_stats(conn, competition_id, season_id):
    """Fetch and store player season stats."""
    print(f"📡 Fetching player season stats...")
    try:
        stats = sb.player_season_stats(
            competition_id=competition_id,
            season_id=season_id,
            creds=STATSBOMB_CREDS
        )
    except Exception as e:
        print(f"   ⚠️  Error fetching player season stats: {e}")
        return 0

    if isinstance(stats, pd.DataFrame) and stats.empty:
        print("   ⚠️  No player season stats available")
        return 0

    cursor = conn.cursor()
    count = 0

    for _, row in stats.iterrows():
        sb_player_id = int(row.get('player_id', 0))
        player_name = str(row.get('player_name', ''))

        # Ensure player exists
        cursor.execute(
            f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
            (sb_player_id,)
        )
        result = cursor.fetchone()
        if result:
            player_internal_id = result[0]
        else:
            cursor.execute(f"""
                INSERT INTO {table("players")} 
                (statsbomb_player_id, statsbomb_player_name, player_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (statsbomb_player_id) DO NOTHING
            """, (sb_player_id, player_name, player_name))
            cursor.execute(
                f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s",
                (sb_player_id,)
            )
            result = cursor.fetchone()
            player_internal_id = result[0] if result else None

        if not player_internal_id:
            continue

        # Get team internal id
        team_internal_id = None
        if pd.notna(row.get('team_name')):
            cursor.execute(
                f"SELECT team_id FROM {table('teams')} WHERE team_name = %s",
                (str(row['team_name']),)
            )
            team_result = cursor.fetchone()
            if team_result:
                team_internal_id = team_result[0]

        def sg(col, default=None):
            """Safe get from row - returns scalar (avoids array unpacking in SQL)."""
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            if isinstance(val, np.ndarray):
                return val.item() if val.size == 1 else default
            return val

        # Store raw stats as JSON (handle numpy for json.dumps)
        def _to_json_val(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, np.ndarray):
                return v.tolist() if v.size > 0 else None
            return v
        raw_json = json.dumps({k: _to_json_val(v) for k, v in row.items()
                               if _to_json_val(v) is not None}, default=str)

        cursor.execute(f"""
            INSERT INTO {table("player_season_stats")} 
            (player_id, statsbomb_player_id, team_id, competition_id, season_id,
             minutes_played, nineties_played, appearances, starting_appearances,
             goals_90, np_goals_90, np_xg_90, np_xg_per_shot, shots_90,
             shot_on_target_ratio, conversion_ratio,
             assists_90, xa_90, key_passes_90, passing_ratio, passes_90,
             long_balls_90, long_ball_ratio, crosses_90, crossing_ratio,
             passes_into_box_90, through_balls_90, deep_progressions_90,
             dribbles_90, dribble_ratio, carries_90, carry_length,
             turnovers_90, dispossessions_90,
             tackles_90, interceptions_90, tackles_and_interceptions_90,
             clearances_90, blocks_per_shot, pressures_90, counterpressures_90,
             dribbled_past_90, fouls_90,
             obv_90, obv_pass_90, obv_shot_90, obv_defensive_action_90,
             obv_dribble_carry_90,
             save_ratio, goals_faced_90, gsaa_90,
             raw_stats_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            player_internal_id, sb_player_id, team_internal_id,
            competition_id, season_id,
            sg('player_season_minutes'),
            sg('player_season_90s_played'),
            sg('player_season_appearances'),
            sg('player_season_starting_appearances'),
            sg('player_season_goals_90'),
            sg('player_season_npg_90'),
            sg('player_season_np_xg_90'),
            sg('player_season_np_xg_per_shot'),
            sg('player_season_np_shots_90'),
            sg('player_season_shot_on_target_ratio'),
            sg('player_season_conversion_ratio'),
            sg('player_season_assists_90'),
            sg('player_season_xa_90'),
            sg('player_season_key_passes_90'),
            sg('player_season_passing_ratio'),
            sg('player_season_op_passes_90'),
            sg('player_season_long_balls_90'),
            sg('player_season_long_ball_ratio'),
            sg('player_season_crosses_90'),
            sg('player_season_crossing_ratio'),
            sg('player_season_passes_into_box_90'),
            sg('player_season_through_balls_90'),
            sg('player_season_deep_progressions_90'),
            sg('player_season_dribbles_90'),
            sg('player_season_dribble_ratio'),
            sg('player_season_carries_90'),
            sg('player_season_carry_length'),
            sg('player_season_turnovers_90'),
            sg('player_season_dispossessions_90'),
            sg('player_season_tackles_90'),
            sg('player_season_interceptions_90'),
            sg('player_season_tackles_and_interceptions_90'),
            sg('player_season_clearance_90') or sg('player_season_padj_clearances_90'),
            sg('player_season_blocks_per_shot'),
            sg('player_season_pressures_90'),
            sg('player_season_counterpressures_90'),
            sg('player_season_dribbled_past_90'),
            sg('player_season_fouls_90'),
            sg('player_season_obv_90'),
            sg('player_season_obv_pass_90'),
            sg('player_season_obv_shot_90'),
            sg('player_season_obv_defensive_action_90'),
            sg('player_season_obv_dribble_carry_90'),
            sg('player_season_save_ratio'),
            sg('player_season_goals_faced_90'),
            sg('player_season_gsaa_90'),
            raw_json
        ))
        count += 1

    conn.commit()
    print(f"✅ Stored season stats for {count} players")
    return count


# ============================================================
# 7. Mise à jour infos joueur (StatsBomb player mapping API)
# 7. 从 StatsBomb player mapping API 更新球员身高、体重、生日等
# ============================================================

def update_player_info_from_mapping(conn, competition_id, season_id):
    """
    Use the StatsBomb player mapping API to update player biographical info.
    This provides height, weight, DOB, preferred foot, etc.
    """
    print("📡 Fetching StatsBomb player mapping data...")
    try:
        import requests
        from requests.auth import HTTPBasicAuth

        url = (
            f"https://data.statsbomb.com/api/v1/player-mapping"
            f"?competition-id={competition_id}&season-id={season_id}"
            f"&all-account-data=true"
        )
        response = requests.get(
            url,
            auth=HTTPBasicAuth(
                STATSBOMB_CREDS['user'],
                STATSBOMB_CREDS['passwd']
            )
        )

        if response.status_code != 200:
            print(f"   ⚠️  Player mapping API returned status {response.status_code}")
            return 0

        mapping_data = response.json()
        cursor = conn.cursor()
        count = 0

        for entry in mapping_data:
            # API Player Mapping v1: offline_player_id is StatsBomb player ID
            sb_player_id = entry.get('offline_player_id')
            if sb_player_id is None:
                continue

            # API fields: player_birth_date, player_height, player_weight,
            # player_preferred_foot (doc typo: perferred), country_of_birth_name
            cursor.execute(f"""
                UPDATE {table("players")} SET
                    date_of_birth = COALESCE(%s, date_of_birth),
                    height_cm = COALESCE(%s, height_cm),
                    weight_kg = COALESCE(%s, weight_kg),
                    preferred_foot = COALESCE(%s, preferred_foot),
                    nationality = COALESCE(%s, nationality),
                    player_nickname = COALESCE(%s, player_nickname),
                    updated_at = NOW()
                WHERE statsbomb_player_id = %s
            """, (
                entry.get('player_birth_date'),
                entry.get('player_height'),
                entry.get('player_weight'),
                entry.get('player_preferred_foot') or entry.get('player_perferred_foot'),
                entry.get('country_of_birth_name'),
                entry.get('player_nickname'),
                sb_player_id
            ))
            if cursor.rowcount > 0:
                count += 1

        conn.commit()
        print(f"✅ Updated biographical info for {count} players from mapping API")
        return count

    except Exception as e:
        print(f"   ⚠️  Error fetching player mapping: {e}")
        return 0


# ============================================================
# Fonction principale / 主入口
# ============================================================

def run_statsbomb_ingestion(conn=None, max_event_matches=None):
    """
    Run the full StatsBomb data ingestion pipeline.

    Args:
        conn: PostgreSQL connection (creates one if None)
        max_event_matches: Limit number of matches for event ingestion (for testing)

    Returns:
        tuple: (competition_id, season_id, matches_df)
    """
    if conn is None:
        conn = get_connection()

    print("\n" + "="*60)
    print("🏟️  STATSBOMB DATA INGESTION")
    print("="*60)

    # 1. Get competitions
    comps_df = get_competitions()
    ingest_competitions(conn, comps_df)

    # 2. Find target competition/season
    comp_id, season_id, season_name = find_ligue1_current_season(comps_df)

    # 3. Ingest matches
    matches_df = ingest_matches(conn, comp_id, season_id)

    # 4. Ingest events (only matches with available event data)
    if not matches_df.empty:
        matches_avail = matches_df[
            matches_df['match_status'].fillna('').str.lower() == 'available'
        ] if 'match_status' in matches_df.columns else matches_df
        if matches_avail.empty:
            matches_avail = matches_df
        ingest_events(conn, matches_avail, max_matches=max_event_matches)

    # 4b. Ingest match lineups (same filter)
    if not matches_df.empty:
        matches_avail = matches_df[
            matches_df['match_status'].fillna('').str.lower() == 'available'
        ] if 'match_status' in matches_df.columns else matches_df
        if matches_avail.empty:
            matches_avail = matches_df
        ingest_match_lineups(conn, matches_avail, max_matches=max_event_matches)

    # 5. Ingest player season stats
    ingest_player_season_stats(conn, comp_id, season_id)

    # 6. Update player info from mapping
    update_player_info_from_mapping(conn, comp_id, season_id)

    print("\n✅ StatsBomb ingestion complete!")
    return comp_id, season_id, matches_df


if __name__ == "__main__":
    conn = get_connection()
    from src.database import create_schema
    create_schema(conn)
    run_statsbomb_ingestion(conn, max_event_matches=3)
    conn.close()
