# Mini-Projet : Base de Données Multi-Sources Football

**Construction d'une base de données centralisant et reliant les données football de StatsBomb, SkillCorner et Transfermarkt pour la Ligue 1 (saison actuelle).**

---

## Livrable — Vue d'ensemble

Ce repo contient le code du pipeline d'ingestion, le schéma de la base, les instructions de lancement et la justification des choix techniques.

| Élément | Emplacement |
|---------|-------------|
| Schéma détaillé | [SCHEMA.md](SCHEMA.md) |
| Lancement | Section 2 ci-dessous |
| Choix techniques | Section 3 ci-dessous |

---

## 1. Schéma de la base de données

La base PostgreSQL (schéma `fc_metz`) relie **matches**, **équipes**, **joueurs**, **events** et **données physiques** selon le modèle suivant.

### Tables principales

| Table | Source(s) | Rôle |
|-------|-----------|------|
| `competitions`, `seasons` | StatsBomb | Compétition et saison (Ligue 1) |
| `teams` | SB + SC + TM | Équipes avec IDs des 3 sources |
| `matches` | SB + SC | Matchs avec home/away, date, score |
| **`players`** | **SB + SC + TM** | **Table centrale** : infos brutes des 3 sources |
| `player_manual_mapping` | Manuelle | Correspondances SB/SC/TM validées à la main (priorité avant fuzzy) |
| `player_id_mapping` | Interne | Correspondance des IDs calculée |
| `player_fused` | Dérivée | Vue agrégée par joueur (stats + physical + contexte) |
| `events` | StatsBomb | Passes, tirs, duels, etc. |
| `player_season_stats` | StatsBomb | Stats agrégées (goals_90, xG, passes...) |
| `player_match_physical` | SkillCorner | Distance, sprints, vitesse par match |

### `players` vs `player_fused`

| | `players` | `player_fused` |
|---|-----------|----------------|
| **Rôle** | Stockage des données brutes fusionnées | Vue agrégée pour l'analyse |
| **Contenu** | Colonnes par source (IDs, nom, valeur, contrat...) | Moyennes (distance, sprints), stats par 90 min, valeur |
| **Construction** | Remplie par ingestion + ID mapping | Recréée à chaque run par `data_fusion.build_player_fused()` |

**`player_fused`** agrège pour chaque joueur : `player_season_stats` (StatsBomb), `player_match_physical` (SkillCorner), et les champs Transfermarkt de `players`.

---

## 2. Comment lancer le code

### Prérequis

- Python 3.8+
- PostgreSQL installé et démarré
- Credentials API (fournis par email séparé)

### Installation

```bash
cd Algo_FC_Metz
python -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate
pip install -r requirements.txt
cp config.example.py config.py
# Éditer config.py : StatsBomb, SkillCorner, PostgreSQL
```

### Lancer le pipeline

```bash
# Pipeline complet
python main.py

# Réinitialiser la base
python main.py --reset

# Mode rapide (5 matchs events)
python main.py --quick

# Modules séparés
python main.py --statsbomb
python main.py --skillcorner
python main.py --transfermarkt
python main.py --mapping

# ID mapping + exporter candidats pour revue manuelle
python main.py --mapping --export-candidates

# Résumé de la base
python main.py --summary
```

### Commandes supplémentaires

```bash
# Transfermarkt détaillé (contrat, agent)
python main.py --transfermarkt --detailed-tm

# Remplir les nulls Transfermarkt (incrémentale)
python main.py --fill-tm-nulls

# Backfill
python backfill/backfill_lineups.py
python backfill/backfill_lineups.py --limit 20
python backfill/backfill_teams_sc_mapping.py
python backfill/backfill_physical_from_raw.py

# Mappings manuels TM↔SB (après revue des candidats)
python backfill/backfill_manual_tm_mappings.py

# Analyse FC Metz
python data_metz/analyse_metz.py
```

---

## 3. Choix techniques

### Base de données : PostgreSQL

- Schéma relationnel, clés étrangères
- Support des volumétries (events, physical)
- Transactions, isolation

### Architecture du pipeline

- **Modulaire** : un script par source (`statsbomb_ingestion`, `skillcorner_ingestion`, `transfermarkt_scraper`)
- **Relançable** : `INSERT ... ON CONFLICT`, mise à jour incrémentale sans doublons
- **Configuration** : `config.py` centralise StatsBomb, SkillCorner, PostgreSQL

### Gestion des IDs multi-sources

- **Problème** : un même joueur a des IDs différents selon la source
- **Stratégie** : matching par nom (exact → famille → fuzzy), table `player_manual_mapping` pour les paires validées à la main
- **Ordre** : mappings manuels appliqués en priorité, puis fuzzy matching (seuils 0,60 SB↔SC, 0,65 TM↔SB)
- **Traçabilité** : `player_id_mapping` avec `mapping_method` et `confidence`

### Nettoyage et standardisation

- **`id_mapping.py`** : `normalize_name()` (accents NFKD, traits d'union → espaces), `name_similarity()` (Jaccard + Levenshtein)
- **Chaque ingestion** : `strip()`, `lower()` lors du parsing

---

## 4. ID mapping — sorties et workflow manuel

L'étape ID mapping génère quatre fichiers dans `docs/` (mis à jour à chaque run ou avec `--export-candidates`) :

| Fichier | Contenu | Commande |
|---------|---------|----------|
| `missing_sc_players.txt` | Joueurs sans SkillCorner ID | `python main.py` ou `--mapping` |
| `missing_tm_players.txt` | Joueurs sans Transfermarkt ID | idem |
| `candidates_sb_sc.txt` | Paires SB↔SC (similarité 0,60–0,65) à valider | `--mapping --export-candidates` |
| `candidates_tm_sb.txt` | Paires TM↔SB (similarité 0,65–0,70) à valider | idem |

**Workflow manuel** : après revue des candidats, si deux joueurs correspondent bien, ajouter la paire dans `MANUAL_TM_SB_PAIRS` de `backfill/backfill_manual_tm_mappings.py`, puis exécuter ce script. Les mappings manuels sont appliqués en priorité à chaque run du pipeline.

---

## 5. Structure du projet

```
Algo_FC_Metz/
├── main.py
├── config.example.py
├── requirements.txt
├── SCHEMA.md
├── src/
│   ├── database.py
│   ├── statsbomb_ingestion.py
│   ├── skillcorner_ingestion.py
│   ├── transfermarkt_scraper.py
│   ├── id_mapping.py        # Matching SB↔SC↔TM, export missing/candidates
│   └── data_fusion.py       # Construction de player_fused
├── backfill/
│   ├── backfill_lineups.py
│   ├── backfill_physical_from_raw.py
│   ├── backfill_teams_sc_mapping.py
│   └── backfill_manual_tm_mappings.py   # Insère les mappings manuels
├── data_metz/
│   ├── analyse_metz.py
│   └── README.md
├── API/
│   └── statsbomb_access_test2.py
└── docs/                    # Fichiers générés (missing_*, candidates_*)
```

---

## 6. Flux de données

```
StatsBomb → competitions, seasons, teams, matches, events, player_season_stats, players
    │
    │ matching par date + noms d'équipes
    ▼
SkillCorner → skillcorner_team_id, skillcorner_match_id, player_match_physical
    │
    │ matching par statsbomb_id ou nom
    ▼
Transfermarkt → market_value, contract_expiry, current_club, agent
    │
    ▼
ID mapping (manuels + fuzzy) → player_id_mapping, export missing/candidates
    │
    ▼
Data fusion → player_fused (vue agrégée par joueur)
```

---

## 7. Données et sources

| Source | Type | Accès |
|--------|------|-------|
| **StatsBomb** | Events, lineups, stats | API `statsbombpy` |
| **SkillCorner** | Données athlétiques | API `skillcorner.client` |
| **Transfermarkt** | Valeur, contrat, âge | Scraping |

---

## 8. Limitations connues

- SkillCorner : couverture limitée, certains joueurs sans `skillcorner_player_id`
- Transfermarkt : contrat/agent sur pages détail → `--detailed-tm` ou `--fill-tm-nulls`
- Events : `player_id` NULL pour Starting XI, Half Start

---

## 9. Publier sur GitHub

```bash
git remote add origin https://github.com/VOTRE_USERNAME/Proj_Multi-Source_Football_FC_Metz_data.git
git push -u origin main
```

---

*Mini-projet Base de Données — Ligue 1 multi-sources*
