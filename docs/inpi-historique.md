# RNE — ce que l'on peut réellement historiser depuis 2023

Qualification en lecture seule du **25 octobre 2026**, sur trois entreprises publiques déjà utilisées pour les essais. Ce petit échantillon permet de réfuter des hypothèses, pas de garantir une couverture nationale ou l'exactitude juridique de tous les états.

## 1. Trois produits à ne plus confondre

| Produit | Source | Ce qu'il permet |
|---|---|---|
| État courant | Stock national + différentiel récent | Dernière fiche disponible, avec limites de couverture explicites |
| Chronologie d'événements | `formality.historique[]`, observations et métadonnées documentaires | Événements/dépôts datés réellement fournis par la source ; pas nécessairement exhaustifs |
| État à une date passée | `GET /companies/{siren}?date=AAAA-MM-JJ` | Comparaisons d'états sur les entreprises/dates où la réponse est qualifiée |

**Le rejeu national de `/companies/diff` depuis 2023 n'est pas, à lui seul, un mécanisme de reconstruction des états de 2023.** Il ne faut pas appeler ses fichiers « snapshots historiques » simplement parce que la fenêtre demandée est ancienne.

## 2. Documentation officielle relue

- [Formalités v5, août 2026](https://www.inpi.fr/sites/default/files/2026-08/documentation%20technique%20API%20formalite%CC%81s_v5.0-AA.pdf), sections B/D/E et annexe 1 : distinction entre différentiel, fiche courante, état antérieur ; dictionnaire `BlocHistorique`.
- [Actes v4, juin 2025](https://www.inpi.fr/sites/default/files/2025-06/documentation%20technique%20API%20Actes%20v4.0.pdf), sections C/D/E : catalogue des actes, dates de dépôt et retraits.

La documentation définit le count comme un **nombre de liasses intégrées**, tout en utilisant les termes résultats/SIREN dans la description du flux. Il ne faut donc pas transformer automatiquement un count en nombre d'entreprises distinctes, d'événements juridiques, de versions passées ou de fiches à télécharger sans qualification du contrat. Les égalités count/pagination observées sur des petites fenêtres ne règlent pas cette ambiguïté pour tous les usages.

## 3. Tests de l'API historique

Cinq dates testées par entreprise : `2023-01-01`, `2023-12-31`, `2024-12-31`, `2025-12-31`, `2026-03-01`. Comparaison du **contenu JSON**, pas seulement de `updatedAt`. Les différences de tableaux sont des différences de réponse, pas automatiquement autant d'événements métier : ordre, corrections et nomenclatures peuvent aussi varier.

| SIREN | Réponses historiques réussies | Différences constatées avec le courant |
|---|---:|---|
| `552100554` | 4/5 | Contenu différent aux quatre dates réussies : activités, établissements et/ou représentation |
| `542107651` | 5/5 | Contenu et historique identiques au courant à toutes les dates testées |
| `542065479` | 2/5 | Contenu différent en fin 2025 et mars 2026 : établissements, objet, représentation et/ou nomenclature |

Sur les **11 réponses historiques réussies**, 6 ont un contenu différent du courant. Les `updatedAt` racine restent pourtant ceux de 2026 : ce timestamp ne suffit donc **ni à valider ni à invalider** un état historique.

Pour `542107651`, la réponse demandée au 31 décembre 2025 conserve notamment l'historique courant contenant 15 entrées intégrées en 2026. Cela ne prouve pas que tous les champs sont faux : certains peuvent être inchangés ou l'historique retourné peut ne pas être filtré. **On ne peut pas certifier la réponse entière comme snapshot point-in-time.** Une date volontairement invalide renvoie HTTP 400 : le paramètre n'est pas simplement ignoré sans validation dans tous les cas.

Quatre requêtes historiques ont échoué pendant la première passe. Une seconde requête sur `542065479` au 31 décembre 2023 confirme HTTP **500** ; ce n'est pas une preuve d'absence de droits ou d'absence d'entreprise à cette date. Ne pas transformer ces erreurs en données manquantes définitivement acquittées.

## 4. Le différentiel ancien peut renvoyer le courant

Sur une recherche limitée à un SIREN, fenêtre 2023 :

- `552100554` et `542065479` : une fiche **identique au JSON courant**, puis une page vide terminale ;
- `542107651` : aucune fiche retournée.

Les filtres SIREN correspondent aux entreprises demandées. Ces recherches sont terminées pour cet échantillon, pas parcourues à l'échelle nationale. Elles montrent qu'une fiche renvoyée par une fenêtre de 2023 ne constitue pas nécessairement un état de 2023.

## 5. Des événements sont déjà dans notre stock Bronze

Lecture ciblée de trois entreprises dans le **stock**, pas dans leur overlay API : un seul part candidat par SIREN, uniquement les groupes de lignes correspondants.

| SIREN | Entrées `historique` dans le stock | Dans la fiche API courante |
|---|---:|---:|
| `552100554` | 0 | 0 |
| `542107651` | 21 | 34 |
| `542065479` | 8 | 14 |

**Il n'est pas nécessaire de retélécharger toutes les fenêtres API depuis 2023 pour commencer à exploiter ces événements.** Le brut original est déjà conservé dans `company_json`. Les nombres stock/courant ne prouvent pas une simple suite d'ajouts : il faut comparer les identifiants et contenus, et gérer corrections/retraits.

Sur les deux historiques présents :

- champs observés : `dateIntegration`, `numeroLiasse`, `codeEvenement`, `dateEffet`, `cheminDateEffet`, `cheminDateEffetId` ;
- 34 entrées pour 24 numéros de liasse et 14 entrées pour 12 numéros : **ne pas dédupliquer uniquement par numéro de liasse** ;
- `dateEffet` présente sur 47 des 48 entrées, `libelleEvenement` absente sur les 48 ; conserver les codes inconnus sans inventer leur libellé ;
- `dateIntegration` et `dateEffet` diffèrent : une intégration en 2025 peut décrire un effet en 2018. Conserver les deux et déclarer la date utilisée par chaque analyse.

Une liste absente/vide ne prouve pas qu'aucun événement n'a eu lieu. Une liste présente n'est pas un journal exhaustif de toutes les valeurs modifiées.

## 6. Pièces et dépôts : complément, pas remplacement

Les trois catalogues `attachments` ont été consultés, **sans télécharger de PDF**. Ils contiennent respectivement 120, 132 et 87 actes. Pour les deux dernières entreprises, 29 et 4 actes sont datés depuis 2023 ; les bilans/bilans saisis comportent aussi des dépôts récents.

Dates de dépôt, dates d'effet et dates de mise à jour technique ne sont pas interchangeables. Un dépôt ne correspond pas nécessairement à un événement unique de `historique`. Les tombstones, confidentialités et restrictions de diffusion restent à respecter. Les filtres des flux documentaires gardent les réserves documentées dans [inpi.md](inpi.md).

## 7. Méthode recommandée, non encore implémentée

1. **Historique d'événements à partir des données déjà acquises** : transformation séparée du stock et des versions Bronze API. Conserver SIREN, numéro de liasse, code brut, dates d'intégration/effet, chemins d'effet, empreinte et provenance exacte (Parquet/ligne/source). Clé et règles de correction à tester ; aucune fusion arbitraire par seul numéro de liasse.
2. **Complément quotidien** : intégrer les événements présents dans les nouvelles fiches et versionner ce que l'on observe réellement, sans effacer les versions précédentes ni inventer les périodes inconnues.
3. **États historiques ciblés** : choisir des entreprises/dates utiles aux analyses, conserver la date demandée et la date de récupération, comparer le contenu et marquer les réponses non qualifiées/erreurs. Un changement entre deux snapshots donne un intervalle d'observation, pas automatiquement la date exacte de l'événement.
4. **Pièces associées utiles** : compléter par métadonnées de dépôts/actes et comptes structurés. PDF/OCR uniquement après justification et accord séparés.
5. **Clarification INPI** avant toute promesse d'exhaustivité : comportement historique avant le premier état disponible, historique non filtré, couverture du stock, unité du count et relation avec les retours du différentiel.

Cette méthode garde l'objectif d'analyse depuis 2023, mais **ne promet pas un registre exhaustif de toutes les modifications de toutes les entreprises depuis 2023**. Le rejeu API actuel reste une réconciliation de couverture de fiches, pas une garantie d'historique juridique. Sa poursuite, son remplacement ou sa pause exigent une décision explicite ; aucun job n'a été modifié pendant cette qualification.

## 8. Actions et limites de cette qualification

- **36 requêtes API** au total, authentifications comprises ; 18 344 916 octets de réponses réussies (~18,3 Mo), plus les petites réponses d'erreur.
- Réponses d'entreprise traitées en mémoire ; seuls schémas, empreintes, comparaisons, compteurs et dates sont conservés dans les rapports locaux `/tmp/rne-history-*.json` et `/tmp/rne-stock-history-check-20261025.json`.
- Trois lectures ciblées du stock via le catalogue GCS ; pas de scan national.
- Aucun payload personnel ajouté à Git/logs, aucun secret affiché/sauvegardé.
- Aucun téléchargement PDF, OCR, écriture GCS, modification de checkpoint, déploiement, changement IAM ou Scheduler.
- **Pas de nouveau connecteur ou ETL historique implémenté** dans cette tâche ; cette note fixe les éléments vérifiés et la stratégie à implémenter ensuite.
