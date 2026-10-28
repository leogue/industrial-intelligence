# Demande de confirmation de couverture du stock RNE

**Statut : brouillon facultatif conservé pour référence ; aucun envoi prévu à la demande de l'utilisateur.** La recherche publique et les nouveaux constats sur les dates internes du ZIP sont dans [inpi.md](inpi.md#recherche-publique-complémentaire-et-datation-interne-du-zip). La couverture reste non certifiée ; le contact n'est pas une condition imposée à la suite du projet.

## Vérifications du 25 octobre 2026

- Pages officielles consultées : [catalogue FTP/SFTP entreprises](https://data.inpi.fr/content/editorial/Serveur_ftp_entreprises), [catalogue API entreprises](https://data.inpi.fr/content/editorial/Acces_API_Entreprises), [accès à l'API formalité/RNE](https://www.inpi.fr/ressources/formalites-dentreprises/acces-lapi-formalite-rne).
- La page FTP/SFTP contient une ancienne référence au stock de mai 2017 ; elle ne certifie pas la couverture de notre fichier de 2026.
- Lecture de la liste racine FTP, de SIZE/MDTM du ZIP de formalités et des **284 octets** de `readme.txt`. Aucun ZIP retéléchargé.
- Fichier toujours proposé : `stock_RNE_formalites_NIVEAU1_20260304_1400.zip`, **15 135 083 838 octets**, MDTM **20261019090647**. Le contenu n'a pas été rehashé ; l'identité complète avec l'archive GCS n'est donc pas redémontrée par ce seul contrôle de métadonnées.
- Le `readme.txt` concerne `stock_RNE_comptes_annuels_20250926_1000_v2.zip`, et la correction des fichiers internes 1 à 10 de **ce stock de comptes**. Il n'établit aucune correction ni borne pour le ZIP de formalités.
- Aucun watermark confirmé dans les sources consultées. Une date du nom de fichier, un MDTM ou un maximum de `updatedAt` ne constituent pas cette confirmation.
- La recherche web supplémentaire du canal de contact a été interrompue par la limite du service de recherche. Aucune adresse email n'est inventée. Transmettre via un canal officiel INPI/DATA INPI vérifié par l'équipe.

## Message prêt à transmettre

**Objet : Stock RNE formalités — date de couverture et borne de reprise API**

Bonjour,

Dans le cadre d'un projet académique, nous utilisons les données publiques RNE pour constituer une base d'entreprises à jour, avec une chaîne simple : stock initial puis actualisation quotidienne via `/api/companies/diff`.

Le FTP `www.inpi.net` met à notre disposition le fichier suivant :

- `stock_RNE_formalites_NIVEAU1_20260304_1400.zip` ;
- taille : 15 135 083 838 octets ;
- MDTM observé : `20261019090647`.

La date du nom du fichier diffère de celle du serveur. Le `readme.txt` disponible décrit une ancienne correction d'un stock de comptes annuels, sans préciser la couverture de ce stock de formalités.

Pour éviter un trou de données entre stock et flux, pourriez-vous nous confirmer :

1. **La date et l'heure de couverture garanties de ce stock, avec le fuseau**, ainsi que son périmètre : entreprises actives uniquement ou également cessées/radiées, état public complet ou livraison partielle/corrective ?
2. **La borne `from` et les précautions de reprise recommandées sur `/api/companies/diff`** après chargement de ce fichier, notamment pour les corrections tardives et suppressions ?
3. Existe-t-il **un stock plus récent ou un manifeste officiel** indiquant cette date de couverture et les règles de renouvellement ?

Nous ne cherchons pas à reconstituer tous les états historiques depuis 2023, mais à conserver correctement le dernier état disponible sans rejouer inutilement plusieurs années de flux.

Merci pour votre aide.

Cordialement,

[Nom / équipe / établissement à compléter]

## Suite après réponse

La réponse doit identifier le fichier et une borne exploitable, pas seulement indiquer que l'API est quotidienne. Après validation : estimer le rattrapage réellement nécessaire, vérifier le passage stock → flux sur un échantillon, puis proposer un plan simple remplaçant le rejeu historique actuel. Ne pas effacer/réinitialiser les checkpoints existants ni déclarer la base entièrement à jour avant cette validation.

Aucun job, checkpoint, secret, droit IAM ou Scheduler n'a été modifié pendant cette recherche. Le quotidien récent et le backfill existants conservent leur configuration actuelle.
