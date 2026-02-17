# Schéma de la base de données

Base de données multi-sources pour la Ligue 1 (saison actuelle).  
Technologie : **PostgreSQL** (schéma `fc_metz`).

---

## 1. Modèle relationnel (diagramme)

```
┌─────────────────┐       ┌─────────────────┐
│  competitions   │───────│    seasons      │
│  (StatsBomb)    │  1:N   │                 │
└─────────────────┘       └────────┬────────┘
                                    │
                                    │ N:1
                                    ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│     teams       │◀──────│    matches      │──────▶│     teams       │
│ (SB + SC + TM)  │ home  │ (SB + SC)       │ away  │ (SB + SC + TM)  │
└────────┬────────┘       └────────┬────────┘       └─────────────────┘
         │                         │
         │                         │ 1:N
         │                         ▼
         │                ┌─────────────────┐       ┌─────────────────────────┐
         │                │     events      │       │  player_season_stats     │
         │                │   (StatsBomb)   │       │      (StatsBomb)         │
         │                └────────┬────────┘       └────────────┬────────────┘
         │                         │                            │
         │                         │ N:1                         │ N:1
         │                         ▼                            ▼
         │                ┌─────────────────────────────────────────────────┐
         └───────────────▶│                    players                        │
         │                │  Fusion SB + SkillCorner + Transfermarkt         │
         │                │  • statsbomb_player_id, skillcorner_player_id,   │
         │                │    transfermarkt_player_id                        │
         │                │  • date_of_birth, nationality, market_value...   │
         │                └──────────────┬──────────────────────────────────┘
         │                               │
         │                               │ 1:1
         │                               ▼
         │                ┌─────────────────────────┐       ┌─────────────────────────────┐
         └───────────────│  player_id_mapping      │       │  player_match_physical     │
         │                │  Correspondance SB↔SC↔TM │       │  (SkillCorner)              │
         │                └─────────────────────────┘       └─────────────────────────────┘
         │                                                              │ N:1
         │                                                              ▼
         │                ┌─────────────────────────────────────────────────┐
         └───────────────▶│               player_fused                       │
                          │  Vue agrégée par joueur (SB + SC + TM)           │
                          │  goals_90, avg_distance, market_value, etc.     │
                          └─────────────────────────────────────────────────┘
```

---

## 2. Tables et description

### Entités de référence

| Table | Source | Rôle |
|-------|--------|------|
| **competitions** | StatsBomb | Compétitions (ex. Ligue 1) |
| **seasons** | StatsBomb | Saisons liées aux compétitions |
| **teams** | SB + SC + TM | Équipes, avec IDs des 3 sources |

### Entités centrales

| Table | Source | Rôle |
|-------|--------|------|
| **matches** | SB + SC | Matchs, liés à home/away team, compétition, saison |
| **players** | SB + SC + TM | **Table focus** : fusion des infos des 3 sources |

### Données détaillées

| Table | Source | Rôle |
|-------|--------|------|
| **events** | StatsBomb | Événements de jeu (passes, tirs, etc.) par match |
| **match_lineups** | StatsBomb | Compositions d'équipe par match |
| **player_season_stats** | StatsBomb | Stats agrégées par joueur/saison (goals_90, xG...) |
| **player_match_physical** | SkillCorner | Données physiques (distance, sprints) par joueur/match |

### Tables de fusion

| Table | Rôle |
|-------|------|
| **player_id_mapping** | Correspondance `player_id` interne ↔ statsbomb_player_id, skillcorner_player_id, transfermarkt_player_id |
| **player_fused** | Vue agrégée par joueur : events + tracking + contexte (TM) |

---

## 3. Table `players` — fusion des 3 sources

La table `players` centralise les informations de **StatsBomb**, **SkillCorner** et **Transfermarkt**.

| Colonne | Source(s) | Description |
|---------|-----------|-------------|
| `player_id` | Interne | Clé primaire (SERIAL) |
| `player_name` | SB / SC / TM | Nom unifié (priorité StatsBomb) |
| `statsbomb_player_id` | StatsBomb | ID source StatsBomb |
| `statsbomb_player_name` | StatsBomb | Nom tel que dans StatsBomb |
| `skillcorner_player_id` | SkillCorner | ID source SkillCorner |
| `skillcorner_player_name` | SkillCorner | Nom tel que dans SkillCorner |
| `transfermarkt_player_id` | Transfermarkt | ID/URL Transfermarkt |
| `date_of_birth` | SB (Player Mapping) / TM | Date de naissance → âge |
| `nationality` | TM | Nationalité |
| `height_cm`, `weight_kg` | SB (Player Mapping) | Données biométriques |
| `primary_position` | SB / SC / TM | Position principale |
| `market_value` | Transfermarkt | Valeur marchande (format brut) |
| `market_value_numeric` | Transfermarkt | Valeur en nombre (€) |
| `contract_expiry` | Transfermarkt | Fin de contrat |
| `current_club` | Transfermarkt | Club actuel |
| `agent` | Transfermarkt | Agent (optionnel) |

**Logique de fusion** :  
- Premier insert depuis StatsBomb (events/lineups).  
- Mise à jour via SkillCorner (skillcorner_player_id) et Transfermarkt (valeur, contrat) par matching sur le nom (exact ou fuzzy).

---

## 4. Correspondance des IDs (`player_id_mapping`)

Chaque source a ses propres identifiants pour un même joueur. La table `player_id_mapping` fait le lien :

| Champ | Description |
|-------|-------------|
| `player_id` | Référence vers `players.player_id` |
| `statsbomb_player_id` | ID StatsBomb |
| `skillcorner_player_id` | ID SkillCorner |
| `transfermarkt_player_id` | ID Transfermarkt |
| `mapping_method` | "exact" / "fuzzy" / "family" |
| `confidence` | Score de confiance (0–1) |

---

## 5. Vue agrégée `player_fused`

Table dérivée calculée par `data_fusion.build_player_fused()` :

| Type | Colonnes |
|------|----------|
| **Identité** | player_id, player_name |
| **StatsBomb** | minutes_played_sb, goals_90, assists_90, np_xg_90, passes_90, tackles_90, pressures_90, obv_90 |
| **SkillCorner** | matches_tracked, avg_total_distance_m, avg_sprinting_m, avg_max_speed_kmh, avg_sprints, avg_high_speed_runs |
| **Transfermarkt** | market_value, market_value_numeric, contract_expiry, current_club |
| **Métadonnées** | has_event_data, has_tracking_data, has_context_data, sources_linked (1–3) |

---

## 6. Contraintes et index

- **Clés étrangères** : matches → teams, seasons, competitions ; events → matches, players ; etc.
- **UNIQUE** : statsbomb_match_id, skillcorner_match_id, statsbomb_player_id, skillcorner_player_id sur les tables concernées.
- **Index** : match_id, player_id sur events, player_match_physical, match_lineups ; statsbomb_player_id, skillcorner_player_id sur players.

---

*Document généré dans le cadre du mini-projet base de données multi-sources — Ligue 1*
