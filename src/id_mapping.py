"""
ID Mapping - association SB/SC/TM par similarité de noms.
球员 ID 映射 - 通过名称相似度关联 StatsBomb / SkillCorner / Transfermarkt

Améliore les correspondances entre sources avec :
- 名称归一化 (accents, traits d'union)
- Alias connus (ex. Warren Zaire Emery ↔ Warren Zaire-Emery)
- 已知别名表（拼写差异、全名/简称）
- Levenshtein-like (difflib) pour typos
- Filtrage par équipe (même club)
"""

import re
import sys
import os
import unicodedata
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table


# =============================================================================
# Alias connus : noms SB ↔ noms SC/TM (variantes orthographiques, surnoms)
# 已知别名表：StatsBomb 与 SkillCorner/Transfermarkt 的拼写变体、全名/简称
# =============================================================================
# Format: (nom_variant_a, nom_variant_b) - les deux sens si besoin
# 格式：(变体a, 变体b) - 双向匹配
PLAYER_NAME_ALIASES = [
    # Surnoms / 简称
    ("conrad jonathan egan riley", "cj egan riley"),
    ("cj egan riley", "conrad jonathan egan riley"),
    ("joel mugisha", "joel mugisha mvuka"),
    ("joel mugisha mvuka", "joel mugisha"),
    # Nom complet ↔ nom court / 全名↔简称
    ("kevin carlos omoruyi benjamin", "kevin omoruyi"),
    ("kevin omoruyi", "kevin carlos omoruyi benjamin"),
    ("leonardo julian balerdi rossa", "leonardo balerdi"),
    ("leonardo balerdi", "leonardo julian balerdi rossa"),
    ("clinton mukoni mata pedro lourenco", "clinton mata"),
    ("clinton mata", "clinton mukoni mata pedro lourenco"),
    ("cristian sleiker casseres yepes", "cristian casseres"),
    ("cristian casseres", "cristian sleiker casseres yepes"),
    ("jhoanner stalin chavez quintero", "jhoanner chavez"),
    ("jhoanner chavez", "jhoanner stalin chavez quintero"),
    ("julien yves remi lopez baila", "julien lopez"),
    ("julien lopez", "julien yves remi lopez baila"),
    ("justin noel kalumba mwana ngongo", "justin kalumba"),
    ("justin kalumba", "justin noel kalumba mwana ngongo"),
    ("kevin daniel van den kerkhof", "kevin van den kerkhof"),
    ("kevin van den kerkhof", "kevin daniel van den kerkhof"),
    ("kabangu alex teixeira", "alex teixeira"),
    ("alex teixeira", "kabangu alex teixeira"),
    ("naime ben zayer said mchindra", "naime said"),
    ("naime said", "naime ben zayer said mchindra"),
    ("naime ben zayer said mchindra", "naime mchindra"),
    ("naime mchindra", "naime ben zayer said mchindra"),
]


def normalize_name(name):
    """
    Normalize un nom de joueur pour le matching flou.
    Supprime accents, traits d'union → espaces, minuscules.

    归一化球员名：去除重音，连字符转为空格，转小写，用于模糊匹配。
    """
    if not name or not isinstance(name, str):
        return ""
    # Remplacer traits d'union et apostrophes par espaces / 连字符、撇号转为空格
    name = name.replace("-", " ").replace("'", " ").replace("‑", " ")
    # Supprimer les accents (NFKD) / 去除重音
    nfkd = unicodedata.normalize('NFKD', name)
    name_ascii = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Conserver uniquement lettres et espaces / 只保留字母和空格
    name_clean = re.sub(r'[^a-z\s]', '', name_ascii.lower())
    return ' '.join(name_clean.split())


def _check_alias(norm1, norm2):
    """
    Vérifie si deux noms normalisés correspondent via la table d'alias.
    检查两个归一化名是否通过别名表匹配。
    """
    for a, b in PLAYER_NAME_ALIASES:
        na, nb = normalize_name(a), normalize_name(b)
        if (norm1 == na and norm2 == nb) or (norm1 == nb and norm2 == na):
            return True
    return False


def name_similarity(name1, name2):
    """
    Calcule la similarité entre deux noms (0 à 1).
    计算两个球员名的相似度，返回 0-1。

    Utilise : alias, correspondance exacte, inclusion, Jaccard sur tokens.
    使用：别名、精确匹配、包含关系、词元 Jaccard。
    """
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    if not n1 or not n2:
        return 0.0

    # Alias connu / 已知别名
    if _check_alias(n1, n2):
        return 0.95

    # Correspondance exacte après normalisation / 归一化后完全一致
    if n1 == n2:
        return 1.0

    # Un nom contient l'autre (ex. "zaire emery" in "warren zaire emery")
    # 一方包含另一方
    if len(n1) >= 4 and n1 in n2:
        return 0.88
    if len(n2) >= 4 and n2 in n1:
        return 0.88

    parts1 = set(n1.split())
    parts2 = set(n2.split())
    if not parts1 or not parts2:
        return 0.0

    # Similarité Jaccard sur les tokens / 词元 Jaccard 相似度
    intersection = parts1 & parts2
    union = parts1 | parts2
    jaccard = len(intersection) / len(union)

    # Bonus si le nom de famille matche / 姓氏匹配加分
    last1 = n1.split()[-1] if n1.split() else ""
    last2 = n2.split()[-1] if n2.split() else ""
    last_match_bonus = 0.25 if last1 == last2 else 0.0

    base_score = min(1.0, jaccard + last_match_bonus)

    # Complément type Levenshtein (difflib) pour typos / 编辑距离补充（处理拼写错误）
    lev_ratio = SequenceMatcher(None, n1, n2).ratio()
    return max(base_score, lev_ratio * 0.92)


def build_player_id_mapping(conn=None):
    """
    Construit et remplit la table player_id_mapping.
    构建并填充 player_id_mapping 表。

    Réconcilie les IDs joueurs des trois sources (SB, SC, TM).
    统一三源（StatsBomb、SkillCorner、Transfermarkt）的球员 ID。
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    print("\n🔗 Building player ID mapping...")

    # Joueurs ayant au moins un ID source / 至少有一个来源 ID 的球员
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


def apply_manual_mappings(conn=None):
    """
    Applique les correspondances manuelles (player_manual_mapping) avant fuzzy.
    在模糊匹配前应用手动映射表。
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT statsbomb_player_id, skillcorner_player_id, transfermarkt_player_id FROM {table('player_manual_mapping')}")
        rows = cursor.fetchall()
    except Exception:
        return 0  # Table peut ne pas exister / 表可能不存在

    applied = 0
    for sb_id, sc_id, tm_id in rows:
        if not sb_id:
            continue
        # Trouver le player_id SB / 查找 SB 球员
        cursor.execute(f"SELECT player_id FROM {table('players')} WHERE statsbomb_player_id = %s", (sb_id,))
        sb_row = cursor.fetchone()
        if not sb_row:
            continue
        sb_pid = sb_row[0]

        # SB <-> SC : assigner sc_id au joueur SB
        if sc_id:
            cursor.execute(
                f"SELECT player_id, skillcorner_player_name, player_name FROM {table('players')} WHERE skillcorner_player_id = %s",
                (sc_id,),
            )
            sc_row = cursor.fetchone()
            sc_name = None
            if sc_row:
                sc_pid, sc_name, _ = sc_row[0], sc_row[1], sc_row[2]
                sc_name = sc_name or sc_row[2]
            if sc_row and sc_row[0] != sb_pid:
                sc_pid = sc_row[0]
                cursor.execute(
                    f"UPDATE {table('players')} SET skillcorner_player_id = NULL, skillcorner_player_name = NULL WHERE player_id = %s",
                    (sc_pid,)
                )
                cursor.execute(
                    f"UPDATE {table('players')} SET skillcorner_player_id = %s, skillcorner_player_name = %s, updated_at = NOW() WHERE player_id = %s",
                    (sc_id, sc_name, sb_pid)
                )
                cursor.execute(f"UPDATE {table('player_match_physical')} SET player_id = %s WHERE player_id = %s", (sb_pid, sc_pid))
                cursor.execute(f"DELETE FROM {table('player_id_mapping')} WHERE player_id = %s", (sc_pid,))
                cursor.execute(f"DELETE FROM {table('player_fused')} WHERE player_id = %s", (sc_pid,))
                cursor.execute(f"DELETE FROM {table('players')} WHERE player_id = %s", (sc_pid,))
                applied += 1
            elif not sc_row:
                cursor.execute(
                    f"""UPDATE {table('players')} SET skillcorner_player_id = %s, updated_at = NOW() WHERE player_id = %s""",
                    (sc_id, sb_pid)
                )
                applied += 1

        # SB <-> TM : copier champs TM vers SB
        if tm_id:
            cursor.execute(
                f"""SELECT player_id, transfermarkt_url, market_value, market_value_numeric,
                           contract_expiry, current_club, jersey_number, nationality,
                           height_cm, preferred_foot, date_of_birth, primary_position, agent
                    FROM {table('players')} WHERE transfermarkt_player_id = %s""",
                (tm_id,),
            )
            tm_row = cursor.fetchone()
            if tm_row:
                cursor.execute(
                    f"""UPDATE {table('players')} SET transfermarkt_player_id = %s,
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
                    WHERE player_id = %s""",
                    (tm_id,) + tuple(tm_row[1:13]) + (sb_pid,),
                )
                applied += 1

    conn.commit()
    if applied:
        print(f"   ✅ Applied {applied} manual mappings")
    return applied


def _get_player_team_ids(cursor, player_id, source):
    """
    Retourne les team_ids associés à un joueur (via lineups ou physical).
    返回球员关联的 team_id 集合（通过 lineups 或 physical）。
    """
    team_ids = set()
    if source == "sb":
        cursor.execute(f"SELECT team_id FROM {table('match_lineups')} WHERE player_id = %s", (player_id,))
        for row in cursor.fetchall():
            if row[0]:
                team_ids.add(row[0])
    else:  # sc
        cursor.execute(
            f"""SELECT m.home_team_id, m.away_team_id FROM {table('player_match_physical')} p
                JOIN {table('matches')} m ON p.match_id = m.match_id
                WHERE p.player_id = %s""",
            (player_id,),
        )
        for row in cursor.fetchall():
            if row[0]:
                team_ids.add(row[0])
            if row[1]:
                team_ids.add(row[1])
    return team_ids


def attempt_fuzzy_matching(conn=None, export_candidates=False):
    """
    Tente de lier les joueurs non appariés par similarité de noms.
    通过名称相似度链接未匹配的球员。

    - SB<->SC : fusionne joueur SC-only dans joueur SB (même personne, noms variés)
    - SB<->SC：将仅 SC 的球员合并入 SB 球员（同一人，名称变体）
    - TM<->SB : copie les champs TM vers le joueur SB correspondant
    - TM<->SB：将 TM 字段复制到对应的 SB 球员

    export_candidates: écrire les paires 0.60-0.65 (SB-SC) et 0.65-0.70 (TM-SB) pour revue manuelle
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()
    print("\n🔍 Attempting fuzzy name matching for unlinked players...")

    # 0. Appliquer les mappings manuels d'abord / 优先应用手动映射
    apply_manual_mappings(conn)

    # 1. SB <-> SC : merge joueur SC-only dans SB quand les noms matchent
    # 1. SB <-> SC：当名称匹配时，将 SC-only 球员合并到 SB 球员
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
    candidates_sb_sc = []  # Pour export revue manuelle / 供人工核验导出

    for sb_player in sb_only:
        sb_pid, sb_pname, sb_sbname, sb_id = sb_player
        search_name = sb_sbname or sb_pname
        sb_teams = _get_player_team_ids(cursor, sb_pid, "sb")

        best_match = None
        best_score = 0

        for sc_player in sc_only:
            sc_pid, sc_pname, sc_scname, sc_id = sc_player
            target_name = sc_scname or sc_pname

            score = name_similarity(search_name, target_name)
            if score < 0.60:
                continue
            # Filtre équipe : si les deux ont des teams, exiger un chevauchement
            # 球队过滤：若双方都有球队信息，要求有交集
            sc_teams = _get_player_team_ids(cursor, sc_pid, "sc")
            if sb_teams and sc_teams and not (sb_teams & sc_teams):
                continue

            if score > best_score:
                best_score = score
                best_match = sc_player

            # Export candidats (0.60 <= score < 0.65) pour revue
            if export_candidates and 0.60 <= score < 0.65:
                candidates_sb_sc.append((search_name, target_name, score, sb_id, sc_id))

        if best_match:
            sc_pid, sc_pname, sc_scname, sc_id = best_match

            # Libérer la contrainte UNIQUE : vider sc_id sur l'ancien avant de l'assigner au nouveau
            # 释放 UNIQUE 约束：在赋值前先清空原记录的 sc_id
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

            # Mettre à jour les références FK vers sb_pid avant suppression
            # 删除前将外键引用更新为 sb_pid
            cursor.execute(f"UPDATE {table('player_match_physical')} SET player_id = %s WHERE player_id = %s", (sb_pid, sc_pid))
            cursor.execute(f"DELETE FROM {table('player_id_mapping')} WHERE player_id = %s", (sc_pid,))
            cursor.execute(f"DELETE FROM {table('player_fused')} WHERE player_id = %s", (sc_pid,))
            cursor.execute(f"DELETE FROM {table('players')} WHERE player_id = %s", (sc_pid,))

            merged += 1
            sc_only = [p for p in sc_only if p[0] != sc_pid]

    # 2. TM <-> SB : copie les champs TM vers SB quand noms matchent (market_value, etc.)
    # 2. TM <-> SB：名称匹配时将 TM 字段（身价等）复制到 SB 球员
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
    candidates_tm_sb = []  # Pour export revue manuelle / 供人工核验导出

    for tm_player in tm_only:
        tm_pid, tm_pname, tm_id, tm_mv = tm_player
        best = None
        best_score = 0
        for sb_player in sb_no_tm:
            sb_pid, sb_pname, sb_id = sb_player
            score = name_similarity(tm_pname or "", sb_pname or sb_player[1] or "")
            if score < 0.65:
                continue
            if score > best_score:
                best_score = score
                best = sb_player
            if export_candidates and 0.65 <= score < 0.70:
                candidates_tm_sb.append((tm_pname, sb_pname or sb_player[1], score, tm_id, sb_id))

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

    # Export candidats pour revue manuelle / 导出待人工核验的候选对
    if export_candidates and (candidates_sb_sc or candidates_tm_sb):
        docs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
        os.makedirs(docs_dir, exist_ok=True)
        if candidates_sb_sc:
            with open(os.path.join(docs_dir, "candidates_sb_sc.txt"), "w", encoding="utf-8") as f:
                f.write("# SB <-> SC 候选对（相似度 0.60–0.65，待人工核验）\n")
                f.write("# Paires SB–SC (similarité 0.60–0.65)\n")
                f.write("-" * 60 + "\n")
                for sb_name, sc_name, score, sb_id, sc_id in candidates_sb_sc:
                    f.write(f"  {score:.2f} | SB:{sb_id} {sb_name!r} <-> SC:{sc_id} {sc_name!r}\n")
        if candidates_tm_sb:
            with open(os.path.join(docs_dir, "candidates_tm_sb.txt"), "w", encoding="utf-8") as f:
                f.write("# TM <-> SB 候选对（相似度 0.65–0.70，待人工核验）\n")
                f.write("# Paires TM–SB (similarité 0.65–0.70)\n")
                f.write("-" * 60 + "\n")
                for tm_name, sb_name, score, tm_id, sb_id in candidates_tm_sb:
                    f.write(f"  {score:.2f} | TM:{tm_id} {tm_name!r} <-> SB:{sb_id} {sb_name!r}\n")
        print(f"   📁 Exported candidates to docs/")

    return merged + tm_merged


def print_database_summary(conn=None):
    """
    Affiche un résumé de toutes les données en base.
    打印数据库中所有数据的汇总。
    """
    if conn is None:
        conn = get_connection()

    cursor = conn.cursor()

    print("\n" + "="*60)
    print("📊 DATABASE SUMMARY")
    print("="*60)

    # Tables et libellés / 表及标签
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
