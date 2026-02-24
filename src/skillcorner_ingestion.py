"""
SkillCorner ingestion - teams, matches, players, physical.
SkillCorner 入库模块 - 球队、比赛、球员、体能数据

Vue d'ensemble / 功能概述 :
  - 通过 SkillCorner API 拉取 Ligue 1 体能/追踪数据
  - 将 teams、matches、players 与 StatsBomb 已有数据关联
  - **Nouvelle méthode / 新方法** : `matching=statsbomb` + statsbomb_id 精确匹配，fallback 按名称
  - 体能数据写入 player_match_physical（距离、速度、冲刺等）

Flux / 执行顺序 :
  find_ligue1_edition → ingest_teams → ingest_matches → ingest_players → ingest_physical_data
"""

import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SKILLCORNER_USERNAME, SKILLCORNER_PASSWORD
from src.database import get_connection, table
from src.id_mapping import normalize_name, name_similarity


def get_client():
    """Create and return a SkillCorner client."""
    from skillcorner.client import SkillcornerClient
    client = SkillcornerClient(
        username=SKILLCORNER_USERNAME,
        password=SKILLCORNER_PASSWORD
    )
    return client


# ============================================================
# 1. Découverte compétitions / saisons / éditions
# 1. 发现联赛、赛季、赛事版本
# ============================================================

def get_seasons(client):
    """Fetch all available seasons from SkillCorner."""
    print(" Fetching SkillCorner seasons...")
    seasons = client.get_seasons()
    print(f"   Found {len(seasons)} seasons")
    return seasons


def get_competitions(client, season_id=None):
    """Fetch competitions, optionally filtered by season."""
    print(" Fetching SkillCorner competitions...")
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
            print(f"    Error fetching competitions for season {season_id}: {e}")
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
                        print(f"   Found edition: {ed_name} (id={ed_id})")
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
                        print(f"  Found latest edition: id={ed_id}")
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
                print(f"   Found edition (fallback): {ed_name} (id={ed_id})")
                return ed_id, None
    except Exception as e:
        print(f"    Fallback failed: {e}")

    return None, None


# ============================================================
# 2. Ingestion des équipes (nouvelle méthode : matching=statsbomb + statsbomb_id)
# 2. 球队入库：优先 statsbomb_id 精确匹配，fallback 按名称
# ============================================================

# Alias connus : abréviation DB <-> racine SC (ex. Rennes/Rennais) / 已知别名
_TEAM_NAME_ALIASES = [("rennes", "rennais")]


def _team_name_matches(db_name, sc_name):
    """Matching strict par nom, éviter faux positifs (ex. Lens vs Alajuelense) / 严格名称匹配"""
    a, b = (db_name or "").lower(), (sc_name or "").lower()
    if a == b:
        return True
    for short, long_root in _TEAM_NAME_ALIASES:
        if (a == short and long_root in b) or (b == short and long_root in a):
            return True
    if a in b:
        return b.startswith(a) or f" {a}" in b or b.endswith(f" {a}")
    if b in a:
        return a.startswith(b) or f" {b}" in a or a.endswith(f" {b}")
    return False


def _find_db_team_by_name(cursor, team_name, sc_team_name):
    """Find DB team matching sc_team_name or team_name. Returns (team_id,) or None."""
    # Try exact match first
    cursor.execute(
        f"SELECT team_id FROM {table('teams')} WHERE LOWER(team_name) = LOWER(%s)",
        (team_name or sc_team_name or "",)
    )
    r = cursor.fetchone()
    if r:
        return r
    # Try matching against all DB teams
    cursor.execute(
        f"SELECT team_id, team_name FROM {table('teams')}"
    )
    for row in cursor.fetchall():
        if _team_name_matches(row[1], team_name or sc_team_name):
            return (row[0],)
    return None


def ingest_teams(conn, client, competition_edition_id):
    """Fetch SkillCorner teams via matching=statsbomb, link by statsbomb_id (précis) or name (fallback)."""
    print(f" Fetching SkillCorner teams (matching=statsbomb + edition {competition_edition_id})...")

    try:
        # 1) Complet matching=statsbomb / 全量
        teams_sb_all = client.get_teams(params={'matching': 'statsbomb'})
        sb_lookup = {t['id']: t for t in teams_sb_all}

        # 2) Équipes Ligue 1 de l'édition / 该赛季球队列表
        teams_ligue1 = client.get_teams(params={'competition_edition': competition_edition_id})

        # 3) Enrichir avec statsbomb_id / 补充 statsbomb_id
        teams = []
        for t in teams_ligue1:
            enriched = sb_lookup.get(t['id'], t)
            teams.append({**t, 'statsbomb_id': enriched.get('statsbomb_id')})
    except Exception as e:
        print(f"  Error fetching teams: {e}")
        return []

    cursor = conn.cursor()
    count_id = 0
    count_name = 0

    for team in teams:
        sc_team_id = team.get('id')
        team_name = team.get('name', '')
        short_name = team.get('short_name', '')
        statsbomb_id = team.get('statsbomb_id')
        if statsbomb_id is not None:
            try:
                statsbomb_id = int(statsbomb_id)
            except (TypeError, ValueError):
                statsbomb_id = None

        existing = None

        # Priorité 1 : match par statsbomb_team_id (ID précis) / 优先 ID 精确匹配
        if statsbomb_id is not None:
            cursor.execute(
                f"SELECT team_id FROM {table('teams')} WHERE statsbomb_team_id = %s",
                (statsbomb_id,)
            )
            existing = cursor.fetchone()

        # Priorité 2 : match par nom (fallback) / 按名称匹配
        if existing is None:
            existing = _find_db_team_by_name(cursor, team_name, short_name or team_name)

        if existing:
            cursor.execute(
                f"UPDATE {table('teams')} SET skillcorner_team_id = %s WHERE team_id = %s",
                (sc_team_id, existing[0])
            )
            if statsbomb_id is not None:
                count_id += 1
            else:
                count_name += 1
        else:
            cursor.execute(f"""
                INSERT INTO {table('teams')} (team_name, skillcorner_team_id)
                VALUES (%s, %s)
                ON CONFLICT (team_name) DO UPDATE SET skillcorner_team_id = EXCLUDED.skillcorner_team_id
            """, (team_name, sc_team_id))
            count_name += 1

    conn.commit()
    print(f" Processed {count_id + count_name} teams (ID précis: {count_id}, par nom: {count_name})")
    return teams


# ============================================================
# 3. Ingestion des matchs
# 3. 比赛入库：按日期+主客队名关联 StatsBomb matches，更新 skillcorner_match_id
# ============================================================

def ingest_matches(conn, client, competition_edition_id):
    """Fetch and link SkillCorner matches to existing StatsBomb matches."""
    print(f" Fetching SkillCorner matches for edition {competition_edition_id}...")

    try:
        matches = client.get_matches(params={'competition_edition': competition_edition_id})
    except Exception as e:
        print(f"   Error fetching matches: {e}")
        return []

    cursor = conn.cursor()
    linked = 0

    for match in matches:
        sc_match_id = match.get('id')
        match_date = match.get('date_time', '')[:10] if match.get('date_time') else ''

        home_team = match.get('home_team', {})
        away_team = match.get('away_team', {})
        home_team = home_team if isinstance(home_team, dict) else {}
        away_team = away_team if isinstance(away_team, dict) else {}
        home_name = home_team.get('name', '')
        away_name = away_team.get('name', '')
        sc_home_id = home_team.get('id')
        sc_away_id = away_team.get('id')

        # Skip if this SkillCorner match_id is already linked
        cursor.execute(
            f"SELECT 1 FROM {table('matches')} WHERE skillcorner_match_id = %s LIMIT 1",
            (sc_match_id,)
        )
        if cursor.fetchone():
            continue

        existing = None

        # Priorité 1 : match par date + skillcorner_team_id (ID précis) / 优先 ID 精确匹配
        if sc_home_id is not None and sc_away_id is not None:
            cursor.execute(f"""
                SELECT m.match_id FROM {table('matches')} m
                JOIN {table('teams')} h ON m.home_team_id = h.team_id
                JOIN {table('teams')} a ON m.away_team_id = a.team_id
                WHERE m.match_date = %s
                AND h.skillcorner_team_id = %s AND a.skillcorner_team_id = %s
            """, (match_date, sc_home_id, sc_away_id))
            existing = cursor.fetchone()

        # Priorité 2 : match par date + noms (fallback) / 按日期+名称
        if existing is None:
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
            # Insert as new match if it can't be linked / 无法关联则插入新比赛
            home_team_id = None
            away_team_id = None
            # Priorité : skillcorner_team_id puis nom / 优先 ID 再按名称
            if sc_home_id is not None:
                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE skillcorner_team_id = %s",
                    (sc_home_id,)
                )
                ht = cursor.fetchone()
                if ht:
                    home_team_id = ht[0]
            if home_team_id is None and home_name:
                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE LOWER(team_name) LIKE %s",
                    (f"%{home_name.lower()}%",)
                )
                ht = cursor.fetchone()
                if ht:
                    home_team_id = ht[0]

            if sc_away_id is not None:
                cursor.execute(
                    f"SELECT team_id FROM {table('teams')} WHERE skillcorner_team_id = %s",
                    (sc_away_id,)
                )
                at = cursor.fetchone()
                if at:
                    away_team_id = at[0]
            if away_team_id is None and away_name:
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
    print(f"Linked {linked} matches with StatsBomb, processed {len(matches)} total")
    return matches


# ============================================================
# 4. Ingestion des joueurs
# 4. 球员入库：按名称匹配已有 players，更新 skillcorner_player_id；新球员则 INSERT
# ============================================================

def ingest_players(conn, client, competition_edition_id):
    """
    Récupère les joueurs SkillCorner et les associe aux joueurs existants par nom.
    拉取 SkillCorner 球员，按名称关联到已有球员。

    Matching : requête SQL d'abord, puis fallback similarité (normalize_name, accents, traits d'union).
    匹配：先 SQL 精确/LIKE，再按相似度（归一化名称、重音、连字符）回退。
    """
    print("📡 Fetching SkillCorner players...")

    cursor = conn.cursor()

    # Précharger les joueurs SB sans SC pour fallback fuzzy / 预加载无 SC 的 SB 球员，用于模糊回退
    cursor.execute(f"""
        SELECT player_id, player_name, statsbomb_player_name
        FROM {table('players')}
        WHERE statsbomb_player_id IS NOT NULL AND skillcorner_player_id IS NULL
    """)
    sb_without_sc = cursor.fetchall()

    # Get all teams for this competition / 获取该赛季所有球队
    cursor.execute(f"SELECT team_name, skillcorner_team_id FROM {table('teams')} WHERE skillcorner_team_id IS NOT NULL")
    sc_teams = cursor.fetchall()

    count = 0
    for team_name, sc_team_id in sc_teams:
        try:
            # Search for players by team
            players = client.get_players(params={'team': sc_team_id})
            time.sleep(0.3)  # Rate limiting
        except Exception as e:
            print(f"  Error fetching players for team {team_name}: {e}")
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

            # Fallback : similarité de noms (accents, traits d'union) / 回退：名称相似度（重音、连字符）
            if existing is None and sb_without_sc:
                sc_name = full_name or player_name
                best = None
                best_score = 0.0
                for sb_pid, sb_pname, sb_sbname in sb_without_sc:
                    db_name = sb_sbname or sb_pname or ""
                    score = name_similarity(sc_name, db_name)
                    if score >= 0.65 and score > best_score:
                        best_score = score
                        best = (sb_pid,)
                if best:
                    existing = best

            display_name = full_name or player_name

            if existing:
                # Update existing player with SkillCorner info / 更新已有球员的 SkillCorner 信息
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
                # Retirer du cache pour éviter double match / 从缓存移除，避免重复匹配
                sb_without_sc = [p for p in sb_without_sc if p[0] != existing[0]]
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
    print(f"Processed {count} players from SkillCorner")
    return count


# ============================================================
# 5. Ingestion des données physiques (tracking)
# 5. 体能数据入库：距离、速度、冲刺等，写入 player_match_physical
# ============================================================

def ingest_physical_data(conn, client, competition_edition_id):
    """
    Fetch physical/tracking data per player per match from SkillCorner.
    This is the core SkillCorner data: speed, distance, sprints, etc.
    """
    print("Fetching SkillCorner physical data...")

    cursor = conn.cursor()

    # Get all teams with SkillCorner IDs
    cursor.execute(
        f"SELECT team_id, team_name, skillcorner_team_id FROM {table('teams')} "
        "WHERE skillcorner_team_id IS NOT NULL"
    )
    sc_teams = cursor.fetchall()

    total_records = 0

    for team_id, team_name, sc_team_id in sc_teams:
        print(f" Fetching physical data for {team_name}...")

        try:
            physical_data = client.get_physical(
                params={'team': sc_team_id}
            )
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"  Error fetching physical data for {team_name}: {e}")
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
                    get_metric(
                        record,
                        "top_speed", "max_speed", "peak_speed", "peak_velocity",
                        "max_speed_kmh", "top_speed_kmh",
                    ),
                    get_metric(
                        record,
                        "average_speed", "avg_speed", "avg_speed_kmh",
                        "mean_speed", "mean_velocity",
                    ),
                    sprint_count,
                    hsr_count,
                    get_metric(
                        record,
                        "acceleration_count", "num_accelerations",
                        "accelerations", "acceleration_count_full_all",
                        "num_accelerations_full_all", "explosive_accelerations",
                    ),
                    get_metric(
                        record,
                        "deceleration_count", "num_decelerations",
                        "decelerations", "deceleration_count_full_all",
                        "num_decelerations_full_all",
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
    print(f"Stored {total_records} physical data records")
    return total_records


# ============================================================
# Fonction principale / 主入口
# ============================================================

def run_skillcorner_ingestion(conn=None):
    """
    Exécute le pipeline complet SkillCorner.
    执行完整 SkillCorner 入库流程。
    Returns: competition_edition_id
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
        print("Could not find Ligue 1 competition edition!")
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
            print(f"  error: {e}")
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

    print("\nSkillCorner ingestion complete!")
    return edition_id


if __name__ == "__main__":
    conn = get_connection()
    from src.database import create_schema
    create_schema(conn)
    run_skillcorner_ingestion(conn)
    conn.close()
