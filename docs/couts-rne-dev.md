# RNE dev — coûts : mesures, tarifs et limites

Contrôle du 23 octobre 2026. **Les montants ci-dessous ne sont pas une facture.** Le CLI confirme le compte de facturation, pas les dépenses détaillées. Aucun dataset/export de facturation n'était visible via `bq ls` dans ce projet ; un export peut exister ailleurs. Le coût net après quotas partagés, crédits, remises et taxes n'a pas été lu. Consulter Facturation → Rapports, filtré sur `industrial-intelligence-dev` ; les écritures de facturation peuvent être retardées.

## Tarifs officiels utilisés, en USD

- [Cloud Run Jobs, Paris Tier 1](https://cloud.google.com/run/pricing) : 0,000018 $/vCPU-seconde et 0,000002 $/GiB-seconde ; minimum d'une minute par instance.
- Quota gratuit Jobs par compte/mois : 240 000 vCPU-secondes et 450 000 GiB-secondes, **partagés avec les autres projets**.
- [GCS Standard EU multi-région](https://cloud.google.com/storage/pricing) : environ 0,026 $/GiB-mois, 0,01 $/1 000 opérations A et 0,0004 $/1 000 opérations B.
- Réplication EU : 0,02 $/GiB écrit ; lectures GCS EU → région Paris : scénario tarifaire 0,02 $/GiB. Tous les transferts ne sont pas classifiés individuellement par les métriques collectées.

Un upload résumable est facturé comme **une** opération A, pas une par chunk. Les erreurs 4xx sont généralement non facturées. Les taux mono-région d'opérations A ne doivent pas être utilisés pour notre bucket EU.

## Traitements déjà réalisés

| Poste | Mesure | Valorisation brute indicative |
|---|---|---|
| Conversion nationale | 7 h 13 min 14 s, 1 vCPU / 2 GiB | **0,572 $** de calcul |
| Transfert ZIP FTP | 11 min 50 s, 1 vCPU / 1 GiB | **0,014 $** de calcul |
| Stockage vivant | 32,053 Go décimaux ≈ 29,85 GiB | **0,776 $/mois**, proratisé à la durée réelle |
| Opérations GCS depuis le 22/10 | écritures, listes, métadonnées et lectures ci-dessous | **0,255 $** |
| Octets reçus par GCS | 32 017 393 882 octets | **0,596 $** de réplication EU estimée |
| Octets envoyés par GCS | 15 239 651 048 octets | **0,284 $** si valorisés EU → Paris ; les quelques lectures locales ont une autre tarification |

Les gros jobs représentent donc environ **0,59 $ de calcul avant gratuité**, pas 20–30 €. Les principaux postes observables ci-dessus totalisent environ **1,7 $ avant gratuités/crédits**, hors autres réseaux/services, stockage dans la durée et taxes. Ce sous-total n'est pas un montant facturé exact.

Cloud Monitoring a renvoyé environ 25 917,77 s pour `rne-materialize`, 673,26 s pour `rne-bootstrap`, et 0,165 s pour les courts processus `rne-refresh` sur `container/billable_instance_time`. Ces métriques ne remplacent pas les SKU de facturation et les minimums d'une minute des smoke tests. La valorisation des gros jobs utilise leurs durées d'exécution comme approximation prudente. Leur usage seul est très inférieur aux quotas gratuits Jobs ; leur coût calcul net pourrait être nul si le quota partagé était disponible.

Requêtes GCS observées, regroupées par méthode/réponse OK : 21 911 WriteObject, 1 ComposeObject, 1 636 ListObjects, 12 SetIamPolicy, 27 387 GetObjectMetadata, 19 944 ReadObject, 35 GetBucketMetadata, 16 GetBucketStorageLayout, 158 GetIamPolicy. Les réponses NOT_FOUND/PERMISSION_DENIED ne sont pas ajoutées aux opérations facturables estimées.

Le bucket est STANDARD/EU, versioning désactivé, soft delete de **7 jours** : des objets remplacés/supprimés peuvent aussi être facturés temporairement, au-delà de l'inventaire vivant. Les métriques et l'inventaire ne constituent pas un audit de tous les coûts du compte.

Autres petits postes : registre RNE d'environ **269 Mo** (sous les 0,5 GiB gratuits s'ils sont disponibles sur le compte), quatre versions de secrets (sous les six gratuites si disponibles), un Scheduler même en pause (sous les trois gratuits si disponibles). Hors gratuité : environ 0,24 $/mois pour les quatre versions et 0,10 $/mois pour le Scheduler, plus registre au-delà du quota. Références : [Artifact Registry](https://cloud.google.com/artifact-registry/pricing), [Secret Manager](https://cloud.google.com/secret-manager/pricing), [Scheduler](https://cloud.google.com/scheduler/pricing).

## Rattrapage ciblé : mesure d'une journée, pas de nouveau bootstrap

Des **counts seuls**, sans téléchargement des entreprises, ont découpé le 21 octobre 2026 UTC en **27 fenêtres sous 10 000 résultats**. Somme des counts : **180 305 retours**, soit **1 815 pages à 100 fiches** et 27 pages vides terminales. Cela a demandé 54 appels de diagnostic, login inclus. Ce total peut compter plusieurs fois une entreprise ou une frontière : ce n'est pas un nombre d'entreprises distinctes.

La temporisation cliente de 0,25 s impose déjà environ **7,6 min pour ces pages de données**, auxquelles ajouter counts, pages vides, réseau, normalisation et GCS. Cela ne donne pas une durée exacte du job. Le chevauchement de 24 h rejoue une partie du travail ; les objets identiques peuvent être réutilisés, sans supposer que toutes les réponses resteront identiques.

Un benchmark borné API → raw GCS → Parquet, depuis le poste local avec ADC, a ensuite traité le créneau `12:00–12:01 UTC` : **91 fiches, 1 page non vide, 802 742 octets bruts, 86 474 octets Parquet**, en **4,762 s**, login, deux counts, page vide et vérification de métadonnées inclus (5 appels API). C'est une mesure locale de petit lot, pas un benchmark Cloud Run représentatif de toutes les pages. Aucun checkpoint principal n'a été créé/modifié.

À titre de calcul : 1 815 pages × quatre écritures principales/page vaut environ **0,073 $ d'opérations A** ; une heure sur 1 vCPU / 1 GiB vaut **0,072 $ de calcul brut**. Le réseau, la réplication et la conservation des données s'ajoutent. **Un rattrapage de quelques jours peut donc rester très peu coûteux**, sans garantir qu'il soit instantané ni extrapoler un unique petit lot à tout le trafic.

## Pourquoi l'ancien scénario complet est différent

Une reconstruction de l'ordre de 27,35 millions de lignes implique **273 500 pages ou davantage**, soit environ **152 fois** le travail d'une journée mesurée. Au rythme de temporisation seul : **19 h minimum**, avant les échanges et les quotas.

Le code actuel sauvegarde chaque page séparément : brut, Parquet, manifeste et checkpoint. Pour 273 500 pages, cela représente environ **1 094 000 écritures principales**, soit **10,94 $ d'opérations A EU** avant réplication, calcul et stockage. Le brut API est actuellement non compressé : un parcours comparable au stock pourrait ajouter de l'ordre de 180–250 Go, au lieu d'un ZIP original de 15 Go. Ce sont surtout cette granularité et ce second chargement intégral qui augmentent le coût, pas les sept heures de CPU du traitement existant.

L'enveloppe précédemment annoncée de 20–30 € concernait ce scénario de reconstruction complète et n'était **ni un devis, ni une facture, ni le coût du quotidien**. Aucun tel chargement n'a été lancé. Un rattrapage ciblé nécessite encore une borne justifiée ou une stratégie de couverture explicitement acceptée ; il ne faut pas assimiler la date de téléchargement du ZIP à sa date de couverture.
