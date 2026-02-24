# Mini-Projet : Base de Données Multi-Sources Football

**Construction d'une base de données centralisant et reliant les données football de StatsBomb, SkillCorner et Transfermarkt pour la Ligue 1 (saison actuelle).**

---

## Livrable — Vue d'ensemble

Ce repo contient le code du pipeline d'ingestion, le schéma de la base, les instructions de lancement et la justification des choix techniques.

| Élément | Emplacement |
|---------|-------------|
| **Schéma de la base** | [docs/SCHEMA.md](docs/SCHEMA.md) |
| **Comment lancer le code** | Section 2 ci-dessous |
| **Choix techniques** | Section 3 ci-dessous |

---

## 1. Schéma de la base de données

La base PostgreSQL (schéma `fc_metz`) relie **matches**, **équipes**, **joueurs**, **events** et **données physiques** selon le modèle suivant.

### Tables principales

| Table | Source(s) | Rôle |
|-------|-----------|------|
| `competitions`, `seasons` | StatsBomb | Compétition et saison (Ligue 1) |
| `teams` | SB + SC + TM | Équipes avec IDs des 3 sources |
| `matches` | SB + SC | Matchs avec home/away, date, score |
| **`players`** | **SB + SC + TM** | **Table centrale** : fusion des infos des 3 sources |
| `events` | StatsBomb | Passes, tirs, duels, etc. |
| `player_season_stats` | StatsBomb | Stats agrégées (goals_90, xG, passes...) |
| `player_match_physical` | SkillCorner | Distance, sprints, vitesse par match |
| `player_id_mapping` | Interne | Correspondance des IDs entre sources |
| `player_manual_mapping` | Optionnel | Correspondances manuelles SB/SC/TM (priorité avant fuzzy) |
| `player_fused` | Dérivée | Vue agrégée par joueur (events + tracking + contexte) |

### Focus table `players`

La table `players` fusionne les informations de StatsBomb, SkillCorner et Transfermarkt :

- **StatsBomb** : `statsbomb_player_id`, nom, date de naissance, taille, position
- **SkillCorner** : `skillcorner_player_id`, nom alternatif
- **Transfermarkt** : `transfermarkt_player_id`, valeur marchande, âge, contrat, club

La correspondance des IDs est gérée par matching sur le nom (exact, famille, fuzzy) et stockée dans `player_id_mapping`.

**Schéma détaillé et diagramme** : [docs/SCHEMA.md](docs/SCHEMA.md)

---

## 2. Comment lancer le code

### Prérequis

- Python 3.8+
- PostgreSQL installé et démarré
- Credentials API (fournis par email séparé)

### Installation

```bash
# Cloner / extraire le repo
cd Algo_FC_Metz

# Créer l'environnement virtuel
python -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt

# Configurer les credentials
cp config.example.py config.py
# Éditer config.py : StatsBomb, SkillCorner, PostgreSQL
```

### Lancer le pipeline

```bash
# Pipeline complet (StatsBomb + SkillCorner + Transfermarkt + mapping + fusion)
python main.py

# Réinitialiser la base (supprime et recrée le schéma fc_metz)
python main.py --reset

# Mode rapide (5 matchs pour tester)
python main.py --quick

# Modules séparés
python main.py --statsbomb      # StatsBomb uniquement
python main.py --skillcorner    # SkillCorner uniquement
python main.py --transfermarkt # Transfermarkt uniquement
python main.py --mapping       # ID mapping uniquement
python main.py --mapping --export-candidates  # + exporter candidats pour revue manuelle

# Résumé de la base
python main.py --summary
```

### Commandes supplémentaires

```bash
# Transfermarkt détaillé (contrat, agent) — plus long
python main.py --transfermarkt --detailed-tm

# Remplir les nulls Transfermarkt (contrat/agent) de façon incrémentale
python main.py --fill-tm-nulls

# Test d'accès StatsBomb
python API/statsbomb_access_test2.py

# Backfill match_lineups (compositions d'équipe) si la table est vide
python backfill/backfill_lineups.py
python backfill/backfill_lineups.py --limit 20  # limite à 20 matchs

# Backfill skillcorner_team_id (corriger les mappings SC/SB avec matching=statsbomb)
python backfill/backfill_teams_sc_mapping.py
```

---

## 3. Choix techniques

### Base de données : PostgreSQL

- **Cohérence** : schéma relationnel clair avec clés étrangères
- **Évolutivité** : support des grandes volumétries (events, physical)
- **Concurrence** : transactions, isolation
- **Alternatives envisagées** : SQLite (limité en écritures concurrentes), DuckDB (orienté analytics)

### Architecture du pipeline

- **Modulaire** : un script par source (`statsbomb_ingestion`, `skillcorner_ingestion`, `transfermarkt_scraper`)
- **Relançable** : `INSERT ... ON CONFLICT`, mise à jour incrémentale
- **Centralisation** : `config.py` pour credentials et paramètres

### Gestion des IDs multi-sources

- **Problème** : un même joueur a des IDs différents selon la source
- **Stratégie** : table `players` avec colonnes `statsbomb_player_id`, `skillcorner_player_id`, `transfermarkt_player_id`
- **Matching** : exact → famille → fuzzy (Jaccard sur tokens, normalisation Unicode)
- **Traçabilité** : table `player_id_mapping` avec `mapping_method` et `confidence`

### Focus sur la table `players`

Conformément au sujet, la table `players` centralise les informations des 3 sources :

| Source | Champs stockés |
|--------|----------------|
| StatsBomb | statsbomb_player_id, nom, date de naissance, taille, poids, position |
| SkillCorner | skillcorner_player_id, nom alternatif |
| Transfermarkt | transfermarkt_player_id, valeur marchande, contrat, club, agent |

La vue `player_fused` agrège les stats (events, physical) et le contexte (TM) par joueur pour l’analyse.

---

## 4. Structure du projet

```
Algo_FC_Metz/
├── main.py                 # Point d'entrée
├── config.py               # Credentials (copier depuis config.example.py)
├── config.example.py       # Template sans credentials
├── requirements.txt
├── docs/
│   └── SCHEMA.md           # Schéma détaillé + diagramme
├── src/
│   ├── database.py         # Schéma PostgreSQL
│   ├── statsbomb_ingestion.py
│   ├── skillcorner_ingestion.py
│   ├── transfermarkt_scraper.py
│   ├── id_mapping.py        # Correspondance SB ↔ SC ↔ TM
│   └── data_fusion.py       # Construction de player_fused
├── backfill/
│   ├── backfill_lineups.py
│   └── backfill_physical_from_raw.py
├── API/
│   └── statsbomb_access_test2.py   # Test accès StatsBomb
└── notebooks/
    └── api_access.ipynb    # Exploration des APIs
```

---

## 5. Flux de données

```
StatsBomb (Ligue 1 France)
    → competitions, seasons, teams, matches, events, player_season_stats, players
              │
              │ matching par date + noms d'équipes
              ▼
SkillCorner (FRA Ligue 1)
    → skillcorner_team_id (matching=statsbomb + statsbomb_id prioritaire), skillcorner_match_id, player_match_physical
              │
              │ matching par statsbomb_id (précis) ou par nom (fallback)
              ▼
Transfermarkt (Ligue 1)
    → market_value, contract_expiry, current_club, agent
              │
              ▼
ID mapping → player_id_mapping
              │
              ▼
Data fusion → player_fused (vue agrégée)
```

---

## 6. Données et sources

| Source | Type | Accès | Documentation |
|--------|------|-------|---------------|
| **StatsBomb** | Events | API `statsbombpy` | Pièces jointes (Competitions, Matches, Events, Player Stats, Player Mapping) |
| **SkillCorner** | Données athlétiques | API `skillcorner.client` | https://skillcorner.com/api/docs/ |
| **Transfermarkt** | Contexte (âge, valeur, contrat) | Scraping | https://www.transfermarkt.fr |

---

## 7. Limitations connues

- **SkillCorner** : couverture limitée (certaines équipes/matchs) ; certains joueurs sans `skillcorner_player_id`
- **Transfermarkt** : contrat/agent sur les pages joueur → option `--detailed-tm` ou `--fill-tm-nulls`
- **Events** : `player_id` peut être NULL pour les événements structurels (Starting XI, Half Start)

---

---

## 8. Publier sur GitHub

```bash
# 1. Créer le dépôt "Proj_Multi-Source_Football_FC_Metz_data" sur GitHub (new repository)

# 2. Ajouter le remote (remplacer VOTRE_USERNAME par votre nom d'utilisateur GitHub)
git remote add origin https://github.com/VOTRE_USERNAME/Proj_Multi-Source_Football_FC_Metz_data.git

# 3. Pousser le code
git push -u origin main
```

---

*Mini-projet Base de Données — Ligue 1 multi-sources*
