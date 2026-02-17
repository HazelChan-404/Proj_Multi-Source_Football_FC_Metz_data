"""
FC Metz - Pipeline principal
Pipeline multi-sources : StatsBomb, SkillCorner, Transfermarkt.
"""

import sys
import os
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import POSTGRES_CONFIG, DB_SCHEMA
from src.database import get_connection, create_schema, reset_database
from src.statsbomb_ingestion import run_statsbomb_ingestion
from src.skillcorner_ingestion import run_skillcorner_ingestion
from src.transfermarkt_scraper import run_transfermarkt_scraping
from src.id_mapping import build_player_id_mapping, attempt_fuzzy_matching, print_database_summary
from src.data_fusion import build_player_fused


def _run_step(name, runner, **kwargs):
    """Exécute une étape du pipeline et capture les erreurs."""
    conn = get_connection()
    try:
        runner(conn, **kwargs)
    except Exception as e:
        print(f"\n❌ {name} error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="FC Metz - Pipeline Ligue 1")
    parser.add_argument('--reset', action='store_true', help='Reset DB avant exécution')
    parser.add_argument('--quick', action='store_true', help='Events: 5 matchs max')
    parser.add_argument('--statsbomb', action='store_true', help='StatsBomb uniquement')
    parser.add_argument('--skillcorner', action='store_true', help='SkillCorner uniquement')
    parser.add_argument('--transfermarkt', action='store_true', help='Transfermarkt uniquement')
    parser.add_argument('--detailed-tm', dest='detailed_tm', action='store_true',
                        help='Transfermarkt: pages détaillées (contrat, agent)')
    parser.add_argument('--fill-tm-nulls', action='store_true',
                        help='Remplir contract/agent manquants')
    parser.add_argument('--fill-tm-limit', type=int, default=None)
    parser.add_argument('--mapping', action='store_true', help='ID mapping uniquement')
    parser.add_argument('--summary', action='store_true', help='Résumé DB uniquement')
    parser.add_argument('--max-events', type=int, default=None, help='Limite matchs events')

    args = parser.parse_args()
    run_specific = (
        args.statsbomb
        or args.skillcorner
        or args.transfermarkt
        or args.mapping
        or getattr(args, "fill_tm_nulls", False)
    )
    run_all = not run_specific and not args.summary

    # --- En-tête ---
    print("=" * 60)
    print(" FC METZ - FOOTBALL DATA PIPELINE")
    print("=" * 60)
    print(f"Database: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    print(f"Schema: {DB_SCHEMA}")
    print(f"Mode: {'Full Pipeline' if run_all else 'Selective'}")
    print()

    start_time = time.time()

    # --- 1. Schema ---
    if args.reset:
        reset_database()
    else:
        conn = get_connection()
        create_schema(conn)
        conn.close()

    if args.summary:
        conn = get_connection()
        print_database_summary(conn)
        conn.close()
        return

    # --- 2. StatsBomb ---
    if run_all or args.statsbomb:
        max_events = args.max_events or (5 if args.quick else None)
        _run_step("StatsBomb", run_statsbomb_ingestion, max_event_matches=max_events)

    # --- 3. SkillCorner ---
    if run_all or args.skillcorner:
        _run_step("SkillCorner", run_skillcorner_ingestion)

    # --- 4. Transfermarkt fill nulls ---
    if getattr(args, "fill_tm_nulls", False):
        conn = get_connection()
        try:
            from src.transfermarkt_scraper import fill_null_transfermarkt_details
            fill_null_transfermarkt_details(conn, max_players=getattr(args, "fill_tm_limit", None))
        except Exception as e:
            print(f"\n❌ Fill TM nulls error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            conn.close()

    # --- 5. Transfermarkt ---
    if run_all or args.transfermarkt:
        _run_step("Transfermarkt", run_transfermarkt_scraping, detailed=getattr(args, 'detailed_tm', False))

    # --- 6. ID mapping ---
    if run_all or args.mapping:
        conn = get_connection()
        try:
            attempt_fuzzy_matching(conn)
            build_player_id_mapping(conn)
        except Exception as e:
            print(f"\n❌ ID mapping error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            conn.close()

    # --- 7. player_fused ---
    conn = get_connection()
    try:
        build_player_fused(conn)
    except Exception as e:
        print(f"\n❌ Data fusion error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    # --- 8. Résumé ---
    conn = get_connection()
    print_database_summary(conn)
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n⏱️  Total time: {elapsed:.1f}s")
    print(f"✅ Pipeline terminé. Schema: {DB_SCHEMA}")


if __name__ == "__main__":
    main()
