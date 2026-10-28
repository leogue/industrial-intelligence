# Audit du volume de réconciliation — 25 octobre 2026

> **Portée de l'audit : coût potentiel d'un rejeu du différentiel, pas estimation d'un historique juridique exhaustif.** La [qualification historique suivante](../rne/inpi-historique.md) confirme qu'une fenêtre ancienne peut renvoyer le courant, que des événements sont déjà dans le stock, et rappelle que la documentation parle de liasses pour le count. Les extrapolations ci-dessous ne constituent donc ni une ETA fiable, ni une justification pour accélérer ce rejeu, ni un nombre de versions historiques distinctes.

## Deux horizons distincts

- Récent collecté jusqu'au **24 octobre 2026 à 00:00 UTC**.
- Historique acquitté jusqu'au **23 janvier 2023 à 12:33:04 UTC**, cible gelée au **24 octobre 2026 à 11:34:50 UTC**.
- La fenêtre historique incomplète a déjà 4 800 retours enregistrés sur 6 464 attendus. Elle reprend au curseur, sans repartir de zéro.

## Mesures sans ingestion

Audit : **953 requêtes HTTP**, uniquement authentification et `/companies/diff/count`. Aucun téléchargement de fiche, écriture GCS, changement de checkpoint/Scheduler ni déploiement. Les requêtes d'analyse sont distinctes du budget configuré dans le job ; elles consomment néanmoins le quota INPI partagé.

Les 46 périodes mensuelles restantes dépassent toutes le plafond de count de 10 000. La réponse globale ne fournit pas de total exact (`isNbResultsOver10000=true`, pas de `nbResults` exploitable).

Une première passe d'échantillonnage horaire avec subdivision exacte a consommé 700 appels ; des fenêtres sondées dans les premiers mois de 2023 totalisent déjà **2 120 169 retours**, pas nécessairement des SIREN distincts. Les pointes dépassent parfois 200 000 retours/heure après subdivision en fenêtres comptables sous le plafond.

Une passe supplémentaire bornée à 250 appels a couvert les mois suivants par échantillonnage horaire et, pour les heures saturées, choix aléatoire de demi-fenêtres avec pondération inverse de la probabilité de sélection. Graines fixes, pas de sélection manuelle des seuls gros counts. Les données et scripts de diagnostic sont locaux sous `/tmp/rne-backfill-*.json`.

## Estimation et limites

L'extrapolation ponctuelle sur la période restante est de l'ordre de **340 millions de retours**. Elle est **très instable**, dominée par quelques grosses opérations en 2023/2024 : peu d'échantillons par mois, fortes variations selon l'heure/jour, répétition possible d'un SIREN entre fenêtres, bornes API et counts évolutifs. Ce n'est ni le nombre d'entreprises françaises ni un total exhaustivement mesuré. Une approximation statistique normale donne environ 110–576 millions, mais n'est pas une garantie de couverture ni une fourchette d'ETA validée.

Débit réel du backfill le 25 octobre : **500 appels, 372 pages dont 366 non vides, 36 365 retours téléchargés**, y compris la partie conservée dans la fenêtre en cours. Le temps de cette phase est d'environ 12 minutes. Ce débit inclut les appels count, subdivisions et pages terminales ; les 500 appels ne produisent donc pas 50 000 lignes utiles garanties.

Au débit observé, exemples arithmétiques, sans promesse :

| Travail restant hypothétique | Exécutions quotidiennes à débit inchangé |
|---|---:|
| 1 million de retours | ~28 jours |
| 10 millions | ~275 jours |
| 100 millions | ~7,5 ans |

L'extrapolation ponctuelle complète donnerait environ 26 ans, **mais elle est trop fragile pour annoncer une date de fin**. Les counts révèlent surtout que **500 appels/jour ne constituent pas une stratégie réaliste de reconstruction historique nationale rapide**. Les limites de temps, quotas, erreurs, modifications des counts et priorité du récent peuvent encore ralentir le traitement.

## Décision à prendre

Ne pas présenter la réconciliation comme presque terminée et ne pas augmenter les ressources CPU pour résoudre un problème de volume/budget API. Revoir la nécessité du rejeu intégral : le stock national est déjà disponible ; rechercher une borne de couverture documentée et une stratégie de rattrapage justifiée, ou qualifier une autre méthode de réconciliation. Toute modification du plan de collecte, de ses budgets et de son coût doit être explicite. Aucun de ces changements n'a été exécuté pendant cet audit.
