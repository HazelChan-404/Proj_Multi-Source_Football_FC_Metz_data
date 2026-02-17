"""
Script de test d'accès à l'API StatsBomb
=========================================
Ce script vérifie les permissions du compte StatsBomb et affiche :
- La liste des compétitions-saisons accessibles
- Le nombre de matchs par compétition
- Le total de matchs disponibles

Utilisation :
    python scripts/test_statsbomb_access.py

Documentation API : https://classic.statsbomb.hudl.com/
"""

import sys
import os

# Ajouter la racine du projet au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from statsbombpy import sb
from config import STATSBOMB_CREDS


def main():
    """Vérifier et afficher l'accès API StatsBomb."""
    creds = STATSBOMB_CREDS

    print("=" * 70)
    print("📡 TEST D'ACCÈS À L'API STATSBOMB")
    print("=" * 70)
    print(f"\nCompte : {creds['user']}")
    print("Vérification des compétitions et matchs accessibles...\n")

    # Récupérer toutes les compétitions-saisons accessibles
    comps = sb.competitions(creds=creds)

    print("-" * 70)
    print("1. LISTE DES COMPÉTITIONS-SAISONS ACCESSIBLES")
    print("-" * 70)
    print(f"Nombre total de combinaisons (compétition + saison) : {len(comps)}\n")

    # Afficher le détail de chaque compétition-saison
    cols = ["competition_name", "season_name", "country_name", "competition_id", "season_id"]
    print(comps[cols].to_string(index=False))
    print()

    # Compter les matchs pour chaque compétition-saison
    print("-" * 70)
    print("2. NOMBRE DE MATCHS PAR COMPÉTITION-SAISON")
    print("-" * 70)

    total = 0
    for _, row in comps.iterrows():
        cid = int(row["competition_id"])
        sid = int(row["season_id"])
        comp_name = row["competition_name"]
        season_name = row["season_name"]

        try:
            matches = sb.matches(competition_id=cid, season_id=sid, creds=creds)
            n = len(matches)
        except Exception as e:
            n = 0
            print(f"   {comp_name} - {season_name} : Erreur ({e})")

        if n > 0:
            total += n
            print(f"  ✓ {comp_name} - {season_name} : {n} matchs")

    print()
    print("-" * 70)
    print("3. RÉSUMÉ")
    print("-" * 70)
    print(f"  Compétitions-saisons accessibles : {len(comps)}")
    print(f"  Nombre total de matchs           : {total}")
    print("=" * 70)
    print("\nTest terminé.\n")


if __name__ == "__main__":
    main()
