# data_metz — Analyse FC Metz

Dossier dédié à l'analyse des données FC Metz pour la saison Ligue 1 2025/2026.

## Fichiers

| Fichier | Description |
|---------|-------------|
| `analyse_metz.py` | Script d'analyse : bilan, classement, joueurs clés |
| `rapport_metz_saison.md` | Rapport synthétique |
| `README.md` | Ce fichier |

## Utilisation

```bash
python data_metz/analyse_metz.py
```

## Données utilisées

- **StatsBomb** : matchs, lineups, events, player_season_stats (goals_90, xG, passes...)
- **SkillCorner** : données physiques (distance, sprint, vitesse)
- **Transfermarkt** : valeur marchande, nationalité, contrat

## Principaux indicateurs

- **Bilan** : V-N-D, buts marqués/encaissés, points
- **Classement** : position en Ligue 1
- **Joueurs** : minutes, goals_90, assists_90, np_xg_90, market_value
