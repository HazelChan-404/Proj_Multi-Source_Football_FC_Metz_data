"""
Transfermarkt scraping - valeur marché, contrat, agent, etc.
Transfermarkt 爬虫模块 - 转会身价、合同、经纪人等

Vue d'ensemble / 功能概述 :
  - 从 Transfermarkt 网页抓取 Ligue 1 球队与球员信息（无 API）
  - 球队列表 → 各队球员 → 球员详情页（合同到期、经纪人、身高、惯用脚等）
  - 存入 players 表，与 StatsBomb/SkillCorner 球员通过姓名匹配关联
  - market_value, contract_expiry, agent, height_cm, preferred_foot 等

Flux / 执行顺序 :
  get_ligue1_teams → get_team_players → [get_player_detail] → store_transfermarkt_data
  → [fill_null_transfermarkt_details 增量补全缺失详情]
"""

import re
import sys
import os
import time
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    TRANSFERMARKT_BASE_URL,
    TRANSFERMARKT_LIGUE1_URL,
    REQUEST_HEADERS,
    INCREMENTAL_UPDATE,
    TRANSFERMARKT_DETAIL_DELAY_SEC,
)
from src.database import get_connection, table


def make_request(url, max_retries=3):
    """Make an HTTP request with retry logic and rate limiting."""
    for attempt in range(max_retries):
        try:
            time.sleep(2 + attempt)  # Respectful rate limiting
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print(f"   ⏳ Rate limited, waiting {10 * (attempt + 1)}s...")
                time.sleep(10 * (attempt + 1))
            elif response.status_code == 403:
                print(f"   ⚠️  Access denied (403) for {url}")
                return None
            else:
                print(f"   ⚠️  HTTP {response.status_code} for {url}")
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Request error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None


# ============================================================
# 1. Récupération des équipes Ligue 1
# 1. 获取 Ligue 1 球队列表（从 Transfermarkt 联赛页面解析）
# ============================================================

def get_ligue1_teams():
    """
    Scrape the Ligue 1 main page to get all team URLs and names.
    Returns list of dicts: [{name, url, tm_id}, ...]
    """
    print("📡 Scraping Transfermarkt Ligue 1 teams...")

    response = make_request(TRANSFERMARKT_LIGUE1_URL)
    if not response:
        print("   ❌ Could not access Transfermarkt Ligue 1 page")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    teams = []

    # Find team links in the main table
    team_links = soup.select('td.hauptlink.no-border-links a[href*="/startseite/verein/"]')
    
    # Also try alternative selectors
    if not team_links:
        team_links = soup.select('table.items a[href*="/verein/"]')
    
    if not team_links:
        team_links = soup.select('a.vereinprofil_tooltip')

    seen_urls = set()
    for link in team_links:
        href = link.get('href', '')
        name = link.get_text(strip=True)

        if not name or not href or href in seen_urls:
            continue
        if '/verein/' not in href:
            continue

        seen_urls.add(href)
        full_url = TRANSFERMARKT_BASE_URL + href if not href.startswith('http') else href

        # Extract team ID from URL
        tm_id_match = re.search(r'/verein/(\d+)', href)
        tm_id = tm_id_match.group(1) if tm_id_match else None

        teams.append({
            'name': name,
            'url': full_url,
            'tm_id': tm_id
        })

    print(f"   Found {len(teams)} teams")
    return teams


# ============================================================
# 2. Récupération des joueurs par équipe
# 2. 从球队页面获取球员列表（姓名、身价、球衣号等）
# ============================================================

def get_team_players(team_url, team_name):
    """
    Scrape a team page to get all players with their basic info.
    Returns list of player dicts.
    """
    # Convert URL to detail/kader (squad) page
    kader_url = team_url.replace('/startseite/', '/kader/')
    if '/kader/' not in kader_url:
        kader_url = team_url

    # Add plus=1 to show detailed view
    if '?' in kader_url:
        kader_url += '&plus=1'
    else:
        kader_url += '?plus=1'

    response = make_request(kader_url)
    if not response:
        print(f"   ❌ Could not access team page for {team_name}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    players = []

    # Find player rows in the squad table
    player_rows = soup.select('table.items tbody tr')

    for row in player_rows:
        try:
            player = parse_player_row(row, team_name)
            if player and player.get('name'):
                players.append(player)
        except Exception:
            continue

    return players


def parse_player_row(row, team_name):
    """Parse a single player row from the Transfermarkt squad table."""
    player = {'current_club': team_name}

    # Player name and link
    name_link = row.select_one('td.hauptlink a[href*="/profil/spieler/"]')
    if not name_link:
        name_link = row.select_one('a.spielprofil_tooltip')
    if not name_link:
        return None

    player['name'] = name_link.get_text(strip=True)
    href = name_link.get('href', '')
    player['url'] = TRANSFERMARKT_BASE_URL + href if not href.startswith('http') else href

    # Extract TM player ID
    tm_id_match = re.search(r'/spieler/(\d+)', href)
    player['tm_id'] = tm_id_match.group(1) if tm_id_match else None

    # Jersey number (skip '-' or non-numeric)
    rn_cell = row.select_one("div.rn_nummer")
    if rn_cell:
        raw = rn_cell.get_text(strip=True)
        if raw and raw != "-":
            try:
                player["jersey_number"] = int(raw)
            except ValueError:
                player["jersey_number"] = raw

    # Position
    pos_cells = row.select('td')
    for cell in pos_cells:
        text = cell.get_text(strip=True)
        # Common positions in French Transfermarkt
        if any(pos in text.lower() for pos in [
            'gardien', 'défenseur', 'milieu', 'attaquant',
            'arrière', 'ailier', 'buteur', 'avant-centre',
            'latéral', 'central', 'offensif', 'défensif',
            'goalkeeper', 'defender', 'midfielder', 'forward',
            'centre-back', 'left-back', 'right-back',
            'centre forward', 'winger'
        ]):
            player['position'] = text
            break

    # Date of birth
    for cell in pos_cells:
        text = cell.get_text(strip=True)
        # Match date patterns like "1 janv. 2000 (25)" or "01/01/2000"
        dob_match = re.search(r'(\d{1,2}\s+\w+\.?\s+\d{4})', text)
        if dob_match:
            player['date_of_birth'] = dob_match.group(1)
            # Extract age
            age_match = re.search(r'\((\d+)\)', text)
            if age_match:
                player['age'] = age_match.group(1)
            break

    # Nationality - from flag images
    flag_imgs = row.select('img.flaggenrahmen')
    if flag_imgs:
        nationalities = []
        for img in flag_imgs:
            nat = img.get('title', '') or img.get('alt', '')
            if nat:
                nationalities.append(nat)
        if nationalities:
            player['nationality'] = ', '.join(nationalities)

    # Market value
    mv_cell = row.select_one('td.rechts.hauptlink a')
    if not mv_cell:
        mv_cell = row.select_one('td.rechts.hauptlink')
    if mv_cell:
        player['market_value'] = mv_cell.get_text(strip=True)
        player['market_value_numeric'] = parse_market_value(player['market_value'])

    # Height
    for cell in pos_cells:
        text = cell.get_text(strip=True)
        height_match = re.search(r'(\d+,\d+)\s*m', text)
        if not height_match:
            height_match = re.search(r'(\d+)\s*cm', text)
        if height_match:
            h = height_match.group(1)
            if ',' in h:
                player['height_cm'] = float(h.replace(',', '.')) * 100
            else:
                player['height_cm'] = float(h)
            break

    # Contract expiry
    for cell in pos_cells:
        text = cell.get_text(strip=True)
        contract_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
        if not contract_match:
            contract_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
        if contract_match:
            player['contract_expiry'] = contract_match.group(1)

    return player


def parse_market_value(value_str):
    """
    Parse market value string to numeric (in euros).
    e.g., "25,00 M €" -> 25000000, "500 K €" -> 500000
    """
    if not value_str:
        return None

    value_str = value_str.strip().lower()

    # Remove currency symbols and whitespace
    value_str = value_str.replace('€', '').replace('$', '').replace('£', '').strip()

    try:
        if 'mrd' in value_str or 'md' in value_str:
            num = re.search(r'([\d,\.]+)', value_str)
            if num:
                return float(num.group(1).replace(',', '.')) * 1_000_000_000
        elif 'mio' in value_str or 'm' in value_str:
            num = re.search(r'([\d,\.]+)', value_str)
            if num:
                return float(num.group(1).replace(',', '.')) * 1_000_000
        elif 'k' in value_str or 'tsd' in value_str:
            num = re.search(r'([\d,\.]+)', value_str)
            if num:
                return float(num.group(1).replace(',', '.')) * 1_000
        else:
            num = re.search(r'([\d,\.]+)', value_str)
            if num:
                return float(num.group(1).replace(',', '.'))
    except (ValueError, AttributeError):
        pass

    return None


# ============================================================
# 3. Détails joueur (page individuelle)
# 3. 球员详情页（合同到期、经纪人、身高、惯用脚、生日等）
# ============================================================

def get_player_detail(player_url):
    """
    Scrape individual player page for detailed info.
    Returns dict with additional player details (contract, agent, height, etc.)
    """
    response = make_request(player_url)
    if not response:
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    details = {}

    # Try multiple info box structures (TM updates layout periodically)
    info_sections = [
        soup.select_one("div.info-table"),
        soup.select_one("table.auflistung"),
        soup.select_one("[data-testid='player-info']"),
    ]
    for info_table in info_sections:
        if not info_table:
            continue
        rows = (
            info_table.select("tr")
            or info_table.select("div.info-table__content")
            or info_table.select("li")
        )
        for row_el in rows:
            label_el = (
                row_el.select_one("th")
                or row_el.select_one("span.info-table__content--regular")
                or row_el.select_one("dt")
            )
            value_el = (
                row_el.select_one("td")
                or row_el.select_one("span.info-table__content--bold")
                or row_el.select_one("dd")
            )
            if label_el and value_el:
                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(strip=True)
                if not value:
                    continue

                if "pied" in label or "foot" in label:
                    details["preferred_foot"] = value
                elif "agent" in label or "conseiller" in label or "player agent" in label:
                    details["agent"] = value
                elif "taille" in label or "height" in label or "size" in label:
                    h_match = re.search(r"(\d+[,.]?\d*)\s*m", value)
                    if h_match:
                        h = h_match.group(1).replace(",", ".")
                        details["height_cm"] = float(h) * 100
                    elif "cm" in value:
                        h_match = re.search(r"(\d+)\s*cm", value)
                        if h_match:
                            details["height_cm"] = float(h_match.group(1))
                elif (
                    "contrat" in label
                    or "contract" in label
                    or "jusqu" in label
                    or "expir" in label
                ):
                    details["contract_expiry"] = value
                elif "naissance" in label or "birth" in label or "né" in label:
                    details["date_of_birth"] = value
                elif "nationalité" in label or "citizenship" in label or "nation" in label:
                    details["nationality"] = value
                elif "position" in label or "poste" in label:
                    details["position"] = value
        if details:
            break

    # Market value (multiple possible locations)
    mv_el = (
        soup.select_one("div.tm-player-market-value-development__current-value")
        or soup.select_one("a.tm-player-market-value-development__current-value")
        or soup.select_one("[data-testid='market-value']")
    )
    if mv_el:
        mv_text = mv_el.get_text(strip=True)
        if mv_text and "€" in mv_text:
            details["market_value"] = mv_text
            details["market_value_numeric"] = parse_market_value(mv_text)

    return details


# ============================================================
# 4. Stockage en base (players + player_id_mapping)
# 4. 入库（更新已有球员或插入新球员，通过姓名匹配关联）
# ============================================================

def store_transfermarkt_data(conn, players_data):
    """Store scraped Transfermarkt data and link to existing players."""
    cursor = conn.cursor()
    linked = 0
    new = 0

    for player in players_data:
        name = player.get('name', '')
        tm_id = player.get('tm_id')
        if not name:
            continue

        # Try to find existing player by name (fuzzy match)
        # Split last name for matching
        name_parts = name.split()
        last_name = name_parts[-1] if name_parts else name

        cursor.execute(f"""
            SELECT player_id FROM {table('players')} 
            WHERE LOWER(player_name) = LOWER(%s)
            OR LOWER(statsbomb_player_name) = LOWER(%s)
            OR LOWER(player_name) LIKE %s
            OR LOWER(statsbomb_player_name) LIKE %s
        """, (
            name, name,
            f"%{last_name.lower()}%",
            f"%{last_name.lower()}%"
        ))
        results = cursor.fetchall()

        # If multiple matches with last name, try more precise match
        if len(results) > 1:
            cursor.execute(f"""
                SELECT player_id FROM {table('players')} 
                WHERE LOWER(player_name) = LOWER(%s)
                OR LOWER(statsbomb_player_name) = LOWER(%s)
            """, (name, name))
            precise_results = cursor.fetchall()
            if precise_results:
                results = precise_results

        if results:
            player_id = results[0][0]
            # Update existing player with Transfermarkt data
            cursor.execute(f"""
                UPDATE {table('players')} SET
                    transfermarkt_player_id = COALESCE(%s, transfermarkt_player_id),
                    transfermarkt_url = COALESCE(%s, transfermarkt_url),
                    market_value = COALESCE(%s, market_value),
                    market_value_numeric = COALESCE(%s, market_value_numeric),
                    contract_expiry = COALESCE(%s, contract_expiry),
                    current_club = COALESCE(%s, current_club),
                    jersey_number = COALESCE(%s, jersey_number),
                    agent = COALESCE(%s, agent),
                    nationality = COALESCE(%s, nationality),
                    height_cm = COALESCE(%s, height_cm),
                    preferred_foot = COALESCE(%s, preferred_foot),
                    date_of_birth = COALESCE(%s, date_of_birth),
                    primary_position = COALESCE(%s, primary_position),
                    updated_at = NOW()
                WHERE player_id = %s
            """, (
                tm_id,
                player.get('url'),
                player.get('market_value'),
                player.get('market_value_numeric'),
                player.get('contract_expiry'),
                player.get('current_club'),
                str(player['jersey_number']) if player.get('jersey_number') is not None else None,
                player.get('agent'),
                player.get('nationality'),
                player.get('height_cm'),
                player.get('preferred_foot'),
                player.get('date_of_birth'),
                player.get('position'),
                player_id
            ))
            linked += 1

            # Update mapping table
            cursor.execute(f"""
                INSERT INTO {table('player_id_mapping')} 
                (player_id, transfermarkt_player_id, mapping_method)
                VALUES (%s, %s, 'name_match')
                ON CONFLICT (player_id) DO UPDATE SET transfermarkt_player_id = EXCLUDED.transfermarkt_player_id
            """, (player_id, tm_id))

        else:
            # Insert as new player
            cursor.execute(f"""
                INSERT INTO {table('players')} 
                (player_name, transfermarkt_player_id, transfermarkt_url,
                 market_value, market_value_numeric, contract_expiry,
                 current_club, jersey_number, nationality, height_cm,
                 preferred_foot, date_of_birth, primary_position, agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                name, tm_id, player.get('url'),
                player.get('market_value'), player.get('market_value_numeric'),
                player.get('contract_expiry'), player.get('current_club'),
                str(player['jersey_number']) if player.get('jersey_number') is not None else None,
                player.get('nationality'),
                player.get('height_cm'), player.get('preferred_foot'),
                player.get('date_of_birth'), player.get('position'),
                player.get('agent')
            ))
            new += 1

    conn.commit()
    print(f"✅ Transfermarkt: linked {linked} existing players, created {new} new entries")
    return linked, new


# ============================================================
# 5. Remplissage des champs manquants (scrape détail incrémental)
# 5. 增量补全（仅对 contract/agent 等仍为空且有 TM URL 的球员爬详情）
# ============================================================

def fill_null_transfermarkt_details(conn=None, max_players=None):
    """
    Incremental: scrape detail pages only for players with transfermarkt_url
    but missing contract_expiry or agent. Does not re-download squad pages.
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT player_id, transfermarkt_url, player_name
        FROM {table('players')}
        WHERE transfermarkt_url IS NOT NULL
        AND (contract_expiry IS NULL OR agent IS NULL)
        LIMIT %s
        """,
        (max_players or 9999,),
    )
    rows = cursor.fetchall()
    if not rows:
        print("   (no players missing contract/agent - skipping detail scrape)")
        return 0

    print(f"   Filling detail for {len(rows)} players missing contract/agent...")
    updated = 0
    for player_id, url, name in rows:
        try:
            details = get_player_detail(url)
            if not details:
                continue
            time.sleep(TRANSFERMARKT_DETAIL_DELAY_SEC)
            cursor.execute(
                f"""
                UPDATE {table('players')} SET
                    contract_expiry = COALESCE(%s, contract_expiry),
                    agent = COALESCE(%s, agent),
                    date_of_birth = COALESCE(date_of_birth, %s),
                    height_cm = COALESCE(height_cm, %s),
                    preferred_foot = COALESCE(preferred_foot, %s),
                    nationality = COALESCE(nationality, %s),
                    primary_position = COALESCE(primary_position, %s),
                    market_value = COALESCE(market_value, %s),
                    market_value_numeric = COALESCE(market_value_numeric, %s),
                    updated_at = NOW()
                WHERE player_id = %s
                """,
                (
                    details.get("contract_expiry"),
                    details.get("agent"),
                    details.get("date_of_birth"),
                    details.get("height_cm"),
                    details.get("preferred_foot"),
                    details.get("nationality"),
                    details.get("position"),
                    details.get("market_value"),
                    details.get("market_value_numeric"),
                    player_id,
                ),
            )
            if cursor.rowcount:
                updated += 1
            if updated % 10 == 0 and updated:
                print(f"      Updated {updated}/{len(rows)}...")
        except Exception as e:
            print(f"      Skip {name}: {e}")
            continue
    conn.commit()
    print(f"   ✅ Filled detail for {updated} players")
    return updated


# ============================================================
# Fonction principale / 主入口
# ============================================================

def run_transfermarkt_scraping(conn=None, detailed=False, fill_nulls_only=False):
    """
    Run the full Transfermarkt scraping pipeline.

    Args:
        conn: SQLite connection
        detailed: If True, scrape individual player pages for contract, agent, etc.
        fill_nulls_only: If True, only scrape detail pages for players missing TM data

    Returns:
        Total number of players processed
    """
    if conn is None:
        conn = get_connection()

    print("\n" + "=" * 60)
    print("💰 TRANSFERMARKT SCRAPING")
    print("=" * 60)

    # 1. Get all Ligue 1 teams
    teams = get_ligue1_teams()

    if not teams:
        print("❌ No teams found on Transfermarkt")
        return 0

    # 2. Store team mapping
    cursor = conn.cursor()
    for team in teams:
        if team.get("tm_id"):
            cursor.execute(
                f"""
                UPDATE {table('teams')} SET transfermarkt_team_id = %s
                WHERE LOWER(team_name) LIKE %s
            """,
                (team["tm_id"], f"%{team['name'].lower().split()[0]}%"),
            )
    conn.commit()

    # 3. Scrape players from each team
    all_players = []
    for i, team in enumerate(teams):
        print(f"\n   [{i+1}/{len(teams)}] Scraping {team['name']}...")

        players = get_team_players(team["url"], team["name"])
        print(f"      Found {len(players)} players")

        if detailed and players:
            to_detail = players if not fill_nulls_only else players
            for j, player in enumerate(to_detail):
                if player.get("url"):
                    details = get_player_detail(player["url"])
                    player.update({k: v for k, v in details.items() if v is not None})
                    time.sleep(TRANSFERMARKT_DETAIL_DELAY_SEC)
                    if (j + 1) % 5 == 0:
                        print(f"         Detail scraped: {j+1}/{len(to_detail)}")

        all_players.extend(players)

    print(f"\n📊 Total players scraped: {len(all_players)}")

    # 4. Store in database
    if all_players:
        store_transfermarkt_data(conn, all_players)

    # 5. Incremental: fill contract/agent for players still missing (no re-squad scrape)
    if detailed or (INCREMENTAL_UPDATE and not fill_nulls_only):
        fill_null_transfermarkt_details(conn)

    print("\n✅ Transfermarkt scraping complete!")
    return len(all_players)


if __name__ == "__main__":
    conn = get_connection()
    from src.database import create_schema
    create_schema(conn)
    run_transfermarkt_scraping(conn, detailed=False)
    conn.close()
