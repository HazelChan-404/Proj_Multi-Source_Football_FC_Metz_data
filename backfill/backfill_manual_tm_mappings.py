"""
Backfill - 插入经人工核验的 TM↔SB 手动映射并应用
Insère les correspondances manuelles TM↔SB validées et les applique
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import get_connection, table
from src.id_mapping import apply_manual_mappings, build_player_id_mapping
from src.data_fusion import build_player_fused

# 经人工核验确认可匹配的 TM↔SB 对
# Paires TM↔SB validées manuellement
MANUAL_TM_SB_PAIRS = [
    (51710, "631927"),   # Saud Abdulhamid (TM) <-> Saud Abdullah Abdul Hamid (SB)
    (449186, "743523"),  # Nathan Buayi-Kiala (TM) <-> Nathan Mbala (SB)
]


def main():
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for sb_id, tm_id in MANUAL_TM_SB_PAIRS:
        cursor.execute(
            f"""SELECT 1 FROM {table('player_manual_mapping')}
                WHERE statsbomb_player_id = %s AND transfermarkt_player_id = %s""",
            (sb_id, tm_id),
        )
        if cursor.fetchone():
            print(f"   Skip (already exists): SB {sb_id} <-> TM {tm_id}")
            continue
        cursor.execute(
            f"""INSERT INTO {table('player_manual_mapping')}
                (statsbomb_player_id, skillcorner_player_id, transfermarkt_player_id, notes)
                VALUES (%s, NULL, %s, %s)""",
            (sb_id, tm_id, f"Manual: SB {sb_id} <-> TM {tm_id}"),
        )
        inserted += 1
    conn.commit()
    print(f"✅ Inserted {inserted} manual TM↔SB mappings")

    # 应用映射：将 TM 字段复制到 SB 球员
    apply_manual_mappings(conn)

    # 重建 player_id_mapping 和 player_fused
    build_player_id_mapping(conn)
    build_player_fused(conn)

    conn.close()
    print("✅ Done. Run `python main.py --summary` to verify.")


if __name__ == "__main__":
    main()
