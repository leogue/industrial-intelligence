# INPI / DATA INPI — exploration du RNE

> Exploration en lecture seule du **22 octobre 2026**, depuis le poste de développement.
> Objectif : comprendre les accès, formats et mécanismes de synchronisation, **pas implémenter ni lancer une ingestion**.
> **Observé** = réponse obtenue avec le compte fourni ; **documenté** = documentation INPI ; **proposé** = choix futur du projet.
>
> Cette note conserve les constats de l'exploration. Un premier connecteur brut pour le stock de formalités et le différentiel entreprises est maintenant décrit dans [le guide d'ingestion](inpi-ingestion.md), avec ses limites et la procédure de déploiement. Cela ne signifie pas qu'il a été exécuté à l'échelle nationale ou déployé.

## 1. Synthèse : à quoi avons-nous réellement accès ?

| Ensemble | Accès vérifié | Format | Intérêt pour Industrial Intelligence |
|---|---|---|---|
| Fiches entreprises RNE | API : HTTP 200, trois SIREN | JSON imbriqué | Identité juridique, activités, établissements, représentants, événements |
| Historique d'une entreprise | API : HTTP 200, un SIREN et une date | JSON | Comparaison d'états ; limites temporelles à approfondir |
| Entreprises créées/modifiées | API différentielle : deux pages testées | JSON `[{"company": {...}}]` | Actualisation incrémentale des fiches |
| Métadonnées des actes | API : par entreprise, par ID et flux | JSON | Catalogue documentaire, dates, types, retraits |
| Actes PDF | Un téléchargement complet vérifié | PDF | Statuts, décisions, modifications ; extraction ciblée ultérieure |
| Métadonnées des comptes annuels | API : par entreprise, par ID et flux | JSON | Exercices, dates de dépôt, confidentialité, retraits |
| Comptes annuels PDF | HTTP 200 et signature PDF ; lecture interrompue à 2 000 001 octets | PDF | Consultation de la pièce originale ; fichiers parfois volumineux |
| Comptes annuels **saisis** | API : par entreprise, par ID et flux | JSON comptable structuré | **Priorité analytique : pas besoin d'OCR** |
| Stock de formalités RNE | FTP : liste, métadonnées, fragments ZIP et premier objet JSON | ZIP64 → tableaux JSON | Bootstrap possible, après vérification du périmètre et de la date du stock |
| Stock de comptes saisis | Même exploration FTP | ZIP64 → tableaux JSON | Bootstrap financier possible |
| SFTP documentaire RNE | **Non vérifié : timeout réseau sur le port 9222, avant authentification** | PDF + MD5 + ZIP selon la documentation | Alternative documentaire ; ne pas confondre avec le FTP |
| Bénéficiaires effectifs / données confidentielles | **Aucun accès privilégié établi** | Accès réglementé | Ne pas présumer leur disponibilité |
| API / FTP de propriété industrielle (PI) | Onglets visibles, **droits et services non testés** | Selon jeux de données | Brevets/marques potentiellement intéressants, mais hors exploration RNE |

**Conclusion :** nous pouvons déjà enrichir SIRENE avec des données juridiques, financières et documentaires. Nous ne disposons pas pour autant d'un graphe complet d'actionnariat.

Cette exploration n'est ni un audit exhaustif des droits du compte, ni une garantie que chaque entreprise, exercice ou document est accessible. Les champs optionnels et les cas réglementaires nécessitent un échantillon plus diversifié.

## 2. Services et authentification

### 2.1 API RNE — vérifiée

Base de production : `https://registre-national-entreprises.inpi.fr/api`.

```http
POST /api/sso/login
Content-Type: application/json

{"username": "<email du compte>", "password": "<secret>"}
```

Réponse observée : objet avec les clés **`token` et `user`**, puis appels avec `Authorization: Bearer <token>`.

- Le JWT contient notamment `exp` et **`expiresIn: 86400`** : durée annoncée de 24 h. `expiresIn` n'était **pas une clé racine** de la réponse de connexion.
- Pas de claim `iat` observé : ne pas calculer une durée avec `exp - iat` si `iat` est absent. Utiliser l'expiration réellement fournie, avec une marge de sécurité.
- Le JWT contient aussi des informations personnelles et des éléments d'authentification imbriqués : **ne jamais l'afficher**, ni journaliser son contenu décodé. Aucun mécanisme de refresh n'a été testé.
- Le jeton a été utilisé uniquement en mémoire et n'a pas été sauvegardé.
- Aucun token API PI ou autre service n'a été demandé : un JWT RNE n'établit pas les droits sur toutes les API INPI.

Rôles techniques retournés dans `user.roles` :

```text
ROLE_NIVEAU_1_COMPANIES_METADATA
ROLE_NIVEAU_1_ACTES_PDF
ROLE_NIVEAU_1_BILANS_PDF
ROLE_NIVEAU_1_BILANS_SAISIES_PDF
ROLE_SFTP
ROLE_API
ROLE_IHM
ROLE_HABILITATION_USER_DEFAULT
```

Ce sont des indices d'habilitation, **pas une preuve d'accès effectif à chaque ressource**. En particulier, `ROLE_SFTP` ne résout pas le timeout réseau constaté.

### 2.2 FTP des stocks — vérifié

- Hôte : **`www.inpi.net`**, protocole **FTP**, port **21**.
- Accès avec le couple technique affiché dans l'espace personnel, **distinct du mot de passe personnel**. Aucun identifiant technique n'est reproduit ici.
- La capture présente ce lien FTP dans un onglet intitulé « Accès SFTP RNE » : l'intitulé de l'interface ne suffit donc pas à identifier le protocole réel.
- FTP classique **non chiffré** : éviter d'y transmettre des secrets personnels. FTPS n'a pas été testé.
- Commandes annoncées par `FEAT` : `EPRT`, `EPSV`, `MDTM`, `PASV`, `REST STREAM`, `SIZE`, `TVFS`, `UTF8`.
- `LIST`, `RETR`, reprise par `REST` et `MDTM` ont fonctionné. `MLSD` a été refusé : prévoir un fallback, pas un client dépendant uniquement de MLSD.

### 2.3 SFTP documentaire — documenté, non validé en direct

- Hôte : `registre-national-entreprises.inpi.fr`, protocole **SFTP**, port **9222**.
- Les documentations SFTP indiquent l'email E-procédures et le mot de passe personnalisé, après acceptation de la licence / activation des accès.
- **Ne pas y utiliser le couple technique du FTP des stocks.**
- Une tentative TCP a expiré avant toute authentification : aucune conclusion possible sur les droits ou la validité du mot de passe SFTP.
- À retester depuis un réseau autorisant ce port ; vérifier l'activation du service et la clé d'hôte SSH avant automatisation. Aucun contournement ni changement réseau n'a été effectué.

## 3. Comment récupérer les données par API

Les chemins ci-dessous sont relatifs à la base `/api`. Ils ont tous été appelés pendant l'exploration, avec les limites indiquées.

| Méthode / chemin | Fonction | Paramètres et particularités |
|---|---|---|
| `GET /companies` | Recherche d'entreprises | `siren[]`, `companyName`, `page`, `pageSize` ; recherche ciblée sur un SIREN testée |
| `GET /companies/{siren}` | Fiche courante | SIREN, chaîne de 9 caractères |
| `GET /companies/{siren}?date=YYYY-MM-DD` | État historique | Une date testée ; ne pas supposer que toutes les dates sont disponibles |
| `GET /companies/diff` | Entreprises nouvelles/modifiées | `from`, `to`, `pageSize`, `searchAfter` |
| `GET /companies/diff/count` | Estimation du nombre de résultats | `nbResults`, `isNbResultsOver10000` ; un créneau d'une heure testé |
| `GET /companies/{siren}/attachments` | Pièces associées | Objet contenant `actes`, `bilans`, `bilansSaisis` |
| `GET /actes/{id}` | Métadonnées d'un acte | Identifiant documentaire, pas SIREN |
| `GET /actes/{id}/download` | Binaire de l'acte | Un PDF complet vérifié |
| `GET /bilans/{id}` | Métadonnées d'un bilan PDF | Identifiant documentaire |
| `GET /bilans/{id}/download` | Binaire du bilan | Signature PDF vérifiée, téléchargement volontairement interrompu |
| `GET /bilans-saisis/{id}` | Bilan structuré | JSON, pas PDF malgré le nom de certains rôles |
| `GET /actes` | Flux documentaire des actes | `dateFrom`, `dateTo`, `pageSize`, `searchAfter` documentés ; réserves §6 |
| `GET /bilans` | Flux des bilans PDF | Mêmes paramètres documentés ; réserves §6 |
| `GET /bilans-saisis` | Flux des bilans structurés | Mêmes paramètres documentés ; réserves §6 |

Autres filtres **documentés**, sans validation exhaustive :

- `/companies` : `submitDateFrom` (immatriculation, inclus), `submitDateTo` (exclus), `numnat`, `activitySectors`, `codeCategory`, `zipCodes[]`.
- `/companies/diff` : `siren[]`, `companyName`, `numnat`, `activitySectors`, `codeCategory`, `zipCodes[]`.
- Les flux documentaires annoncent aussi `siren[]`.
- `codeCategory` est une catégorie d'activité INPI à huit chiffres : **ne pas la confondre avec le code APE**.
- Les listes de formes d'exercice comprennent notamment les activités commerciales, agricoles, artisanales et libérales ; le RNE n'est pas limité aux sociétés commerciales.

### Pagination

- `pageSize` documenté : **1 à 100**, défaut 20. Exploration : 1 ou 2 résultats seulement.
- Recherche `/companies` : pagination `page` / `pageSize` documentée ; une seule page ciblée testée.
- Flux : reprendre la valeur exacte de l'en-tête **`pagination-search-after`** dans `searchAfter`, en conservant les autres filtres.
- Deux pages ont répondu HTTP 200 pour chacun des quatre flux. **Aucun flux n'a été parcouru jusqu'à épuisement**.
- Le curseur des entreprises ressemblait à un SIREN ; ceux des documents à des identifiants de documents. **Les traiter comme opaques**, sans les reconstruire ni les incrémenter.
- La documentation évoque une limite technique de 10 000 résultats et le découpage des fenêtres via `/companies/diff/count`. Ne pas implémenter un bootstrap massif par simple incrémentation de `page`.
- Un créneau `2026-10-20T00:00:00+00:00` → `2026-10-20T01:00:00+00:00` a retourné `nbResults: 2550`, `isNbResultsOver10000: false`. Ce chiffre ponctuel **n'est pas une volumétrie quotidienne**.

## 4. Structure des données récupérables

### 4.1 Fiche RNE : entreprise, formalité et blocs conditionnels

Structure simplifiée **observée**, pas un JSON Schema exhaustif. Les nœuds optionnels diffèrent selon la personne, son historique et les informations diffusibles.

```text
Company
├── id: string                         # identifiant technique INPI
├── siren: string                      # clé de rapprochement avec SIRENE
├── updatedAt: string(datetime offset)
├── nombreRepresentantsActifs: integer
├── nombreEtablissementsOuverts: integer
└── formality: object
    ├── siren: string
    ├── typePersonne: string
    ├── formeJuridique: string
    ├── codeApeNaf2025: string?         # coexistence de nomenclatures
    ├── diffusionINSEE: string
    ├── diffusionCommerciale: boolean
    ├── created: string? / updated: string?
    ├── historique: array?
    │   └── dateIntegration, codeEvenement, numeroLiasse,
    │       dateEffet, cheminDateEffet, cheminDateEffetId: string
    └── content: object
        ├── formeExerciceActivitePrincipale: string
        ├── succursaleOuFiliale: string?
        ├── natureCessation: string?
        ├── natureCreation: object
        │   ├── dateCreation, formeJuridique, formeJuridiqueInsee?: string
        │   └── microEntreprise, societeEtrangere, etablieEnFrance,
        │       salarieEnFrance, entrepriseAgricole, eirl, ...: boolean
        ├── registreAnterieur: object? # RNCS / RNM / RAA
        ├── personneMorale: object?   # sociétés, etc.
        └── personnePhysique: object? # entrepreneurs individuels
```

La documentation décrit également `content.exploitation`, les pièces jointes et d'autres blocs non couverts par notre petit échantillon. L'annexe 1 de la documentation formalités v5 est la référence détaillée des champs publics, **pas cette arborescence abrégée**.

Les blocs de personne morale/physique contiennent notamment :

| Bloc relatif à `formality.content.personneMorale` (ou branche adaptée) | Champs / contenu observés | Utilité / précautions |
|---|---|---|
| `identite.entreprise` | `siren`, `denomination` selon cas, `formeJuridique`, `codeApe`, `codeApeNaf2025` selon cas | Identité et nomenclature ; garder les codes comme chaînes |
| `identite.description` | `objet`, `capitalVariable`, autres attributs selon fiche | Objet social ; ne pas inférer un capital numérique de ce seul indicateur |
| `adresseEntreprise.adresse` | Pays, commune, code postal, code INSEE et éléments de voie selon diffusion | Adresse d'entreprise ; qualité et règles de diffusion à respecter |
| `etablissementPrincipal` | `descriptionEtablissement`, `adresse`, `activites[]` | SIRET et activité du principal |
| `autresEtablissements[]` | Même organisation ; dates de fermeture possibles | Ouverts **et fermés** peuvent coexister |
| `composition.pouvoirs[]` | Rôle, type de personne, individu ou entreprise, état actif selon cas | Représentation légale, pas automatiquement détention du capital |
| `observations.rcs[]` | Observations du registre | Contexte juridique, texte à interpréter avec prudence |
| `detailCessationEntreprise` | `dateEffet`, `dateRadiation` selon cas | Ne pas utiliser la date technique comme date de cessation |

Dans un établissement :

- `descriptionEtablissement.siret`, `codeApe`, éventuellement `codeApeNaf2025`, `dateEffetFermeture` ou `dateEffetTransfert` ;
- `activites[]` : description/catégorisation et codes selon cas ;
- `adresse` : bloc séparé ; **un établissement n'est pas une filiale**.

Dans un pouvoir :

```text
composition.pouvoirs[]
├── roleEntreprise: string
├── typeDePersonne: string
├── representantId: string?
├── actif: boolean?
├── secondRoleEntreprise / libelleSecondRoleEntreprise: string?
├── individu?: {descriptionPersonne, adresseDomicile}
└── entreprise?: {siren, denomination, formeJuridique, ...}
```

Les personnes physiques exposent notamment des noms/prénoms et des informations limitées par la réglementation. Leur présence technique ne justifie pas leur republication systématique. Les nombreux champs `...Present: boolean` ne sont pas la donnée elle-même : **présence, valeur absente, valeur masquée et `false` doivent rester distincts**.

### 4.2 Flux entreprises : enveloppe différente

```json
[
  {
    "company": {
      "id": "<id INPI>",
      "siren": "<9 caractères>",
      "updatedAt": "<horodatage avec fuseau>",
      "formality": {}
    }
  }
]
```

Ne pas réutiliser directement un parseur de `/companies/{siren}` sur les éléments du flux sans gérer l'enveloppe `company`. Le stock FTP échantillonné contenait, lui, des objets entreprise **directs** dans un tableau.

### 4.3 Catalogue des pièces : trois collections distinctes

```text
Attachments
├── actes: ActMetadata[]
├── bilans: BilanMetadata[]
└── bilansSaisis: BilanSaisi[]
```

Pour le SIREN `552100554`, la réponse contenait **120 actes, 7 bilans PDF et 7 bilans saisis**. C'est un exemple ponctuel, pas une moyenne ni une garantie de complétude de tous les exercices. Des données de bilan structuré étaient déjà présentes dans cette réponse : ne pas supposer qu'elle ne contient que des liens.

Métadonnées documentaires :

| Champ | Type observé | Rôle |
|---|---|---|
| `id` | string | Identifiant de ressource ; conserver avec le type `acte` / `bilan` / `bilan_saisi` |
| `siren` | string | Entreprise concernée |
| `updatedAt` | string datetime | Mise à jour technique ; attention aux anomalies des filtres (§6) |
| `dateDepot` | string date, optionnelle | Date métier du dépôt |
| `denomination` | string, optionnelle | Libellé, pas clé |
| `nomDocument` | string, optionnelle | Nom documentaire ; pas un checksum |
| `numChrono`, `numNat`, `numDepot` | string, selon source | Références de dépôt ; ne pas imposer un seul champ à toutes les générations |
| `confidentiality` | string, optionnelle | `Public` observé ; les accès privilégiés restent non établis |
| `deleted` | boolean | Retrait/suppression à traiter explicitement |
| `typeDocument`, `libelle` | string, actes récents | Type de pièce et libellé |
| `typeRdd[]` | objets, actes selon génération | `typeActe`, `decision` optionnelle ; plusieurs types possibles pour un PDF |
| `dateCloture` | string date, bilans | Clôture de l'exercice, distincte du dépôt |
| `typeBilan` | string, bilans | Type comptable ; `C`, `S`, `K` observés sur les échantillons |
| `version` | string, certains bilans | Version fournie, **pas un hash** ni une garantie d'immuabilité |

Les tombstones **réellement observés** dans les flux bilans et bilans saisis ne contenaient que `id`, `deleted`, `updatedAt`, `siren`. Un parseur exigeant toujours `denomination`, `dateDepot` ou `bilanSaisi` rejetterait donc des suppressions importantes.

### 4.4 Bilans saisis : structure financière

```text
BilanSaisi
├── id, siren, updatedAt, dateDepot?, dateCloture?, typeBilan?, ...
├── deleted: boolean
└── bilanSaisi: object?                 # absent sur un tombstone
    ├── version: string
    └── bilan: object
        ├── identite: object
        │   ├── siren, denomination, adresse: string
        │   ├── dateDepot, dateClotureExercice,
        │   │   dateClotureExerciceNMoins1?: string
        │   ├── dureeExerciceN, dureeExerciceNMoins1?: string
        │   ├── codeGreffe?, codeValideur?, numDepot?, numGestion?: string
        │   ├── codeActivite, codeTypeBilan: string
        │   ├── codeDevise, codeOrigineDevise: string
        │   └── codeSaisie, codeConfidentialite, infoTraitement: string
        └── detail: object?            # peut manquer sans erreur réseau
            └── pages: array
                └── numero: integer
                    liasses: array
                    └── code: string
                        m1, m2, m3, m4: string
```

**Il n'existe pas nécessairement de champs explicites `chiffre_affaires` ou `resultat_net`** : les postes sont codés par liasse/formulaire. Conserver le type de bilan, la page, le code de liasse et la version du dictionnaire de décodage.

Exemples **documentés** pour les comptes complets :

| Page / rubrique | `m1` | `m2` | `m3` | `m4` |
|---|---|---|---|---|
| 01 — actif | Brut N | Amortissements N | Net N | Net N−1 |
| 02 — passif | Montant N | Montant N−1 | — | — |
| 03 — résultat | France N | Export N | Total N | Total N−1 |

Ce mapping **n'est pas universel** : autres pages, bilans simplifiés, consolidés, banques, assurances et agricoles ont leurs règles. Se référer aux sections E/F de la documentation comptes v6.

Qualité et confidentialité :

- Les montants sont des **chaînes**, parfois vides, signées ou complétées de zéros. Préserver le brut ; convertir ensuite en décimal, pas en flottant approximatif. Une chaîne vide n'est **pas zéro**.
- Contrôler devise, unité du formulaire et durée d'exercice avant ratios ou comparaisons. Ne pas supposer que toutes les valeurs sont directement en euros ni que tous les exercices durent douze mois.
- `codeSaisie` documenté : `0` sans anomalie ; `1` incohérences source ; `1A` données manquantes ; `6` rescanné. Plusieurs codes de non-saisie ne fournissent que l'identité, sans détail comptable.
- `infoTraitement` documente par exemple une relivraison ou une incohérence de référentiel ; ce n'est pas une donnée financière.
- `codeConfidentialite` documenté : `0` public, `1` confidentiel, `2` résultat confidentiel, `3` publication simplifiée, `4` cas de confidentialité du rapport du commissaire aux comptes. La disponibilité du détail varie ; **un poste absent n'est pas nécessairement une erreur d'ingestion**.
- Une version de bilan ne doit pas être comptée comme un nouvel exercice. Dédupliquer par identifiant source et distinguer les dépôts/corrections avant de choisir une version analytique.

## 5. FTP : ce que contiennent réellement les stocks

### Inventaire observé à la racine

| Fichier | Taille compressée exacte | Date `MDTM` serveur | Entrées ZIP annoncées |
|---|---:|---|---:|
| `readme.txt` | 284 octets | Non interrogée | — |
| `stock_RNE_formalites_NIVEAU1_20260304_1400.zip` | 15 135 083 838 octets (~15,14 Go) | `20261019090647` | 5 470 |
| `stock_RNE_comptes_annuels_NIVEAU1_20260320_1400.zip` | 4 298 343 268 octets (~4,30 Go) | `20260324151059` | 1 437 |

Les nombres d'entrées viennent des structures ZIP64 ; **ce ne sont pas des nombres d'entreprises ou de bilans**. Le répertoire central n'a été lu que partiellement.

Méthode d'exploration :

1. Liste FTP et `MDTM`, lecture du petit `readme.txt`.
2. Pour chaque ZIP : lecture des derniers **65 536 octets** par `REST`, puis d'au plus **65 536 octets** de son répertoire central.
3. Lecture d'un fragment d'au plus **262 144 octets** au début de la première entrée ; décompression bornée à **131 072 octets**.
4. Décodage du premier objet complet du tableau JSON, sans sauvegarder ses valeurs personnelles.

Soit au plus **786 432 octets de fragments ZIP** lus par le client pour les deux stocks, hors échanges de contrôle. **Aucune archive ni entrée JSON complète téléchargée.** Les CRC n'ont donc pas été validés sur les contenus complets.

Résultats :

- Archives **ZIP64**, compression Deflate ; prévoir une bibliothèque compatible avec les archives >4 Go.
- Entrées échantillonnées nommées `stock_000001.json`, `stock_000002.json`, etc.
- Le contenu commence par un **tableau JSON**, pas du JSONL. Utiliser un parseur incrémental de tableaux pour l'ingestion future.
- Premier objet du stock formalités : structure `Company` avec `updatedAt`, `id`, `formality`, dont une branche `personnePhysique`.
- Premier objet du stock comptes : structure de bilan saisi, avec `bilanSaisi.bilan.identite` et `detail.pages[].liasses[]`.
- La première entrée formalités annonce **21 574 205 octets décompressés** ; celle des comptes **79 937 820 octets**. La taille décompressée totale reste **non mesurée**.
- Pas de PDF individuel ni de fichier `.md5` visible à la racine. Cela ne prouve pas leur absence dans toutes les archives : inventaire interne limité.

### Attention à la fraîcheur et aux corrections

Le nom du stock formalités contient mars 2026, mais son `MDTM` indique octobre 2026. **Ni la date du nom ni la date du fichier ne constituent, à elles seules, un watermark métier garanti.**

Le `readme.txt` évoque une ancienne relivraison de septembre 2025 corrigeant uniquement `stock_000001.json` à `stock_000010.json`. Il ne documente pas précisément les ZIP présents en octobre 2026. Cela démontre l'intérêt de gérer les corrections, pas une périodicité de publication.

**Fréquence des stocks actuels : non établie.** Ne pas les qualifier de mensuels ou de snapshots exhaustifs à date connue sans confirmation INPI. Le catalogue éditorial contient aussi une référence historique au stock du 4 mai 2017 : elle ne suffit pas à décrire les archives actuelles.

## 6. Mises à jour, dates, hash et anomalies constatées

### 6.1 Fréquence et signification des dates

| Information | Statut | Utilisation |
|---|---|---|
| API entreprises / actes / comptes : mise à disposition **quotidienne** | Documentation INPI | Batch incrémental quotidien plausible ; pas un SLA ni du streaming |
| SFTP PDF : arborescence `ANNEE/MOIS/JOUR`, date de dépôt | Documentation SFTP 2023 | Découverte documentaire ; traiter les dépôts tardifs et corrections |
| `updatedAt` | Observé | Mise à jour technique de la ressource ; conserver l'offset d'origine et une version UTC |
| `dateDepot`, `dateCloture`, `dateEffet`, `dateIntegration` | Observées selon objets | Dates différentes, ne pas les substituer entre elles |
| `formality.historique[]` | Observé | Événements avec numéro de liasse et dates ; pas preuve d'un journal exhaustif de tous les changements |
| `MDTM` FTP / date du nom du stock | Observés | Indices de publication, pas dates juridiques ni garanties de couverture |
| `retrieved_at` | À produire par notre pipeline | Date UTC à laquelle nous avons effectivement récupéré la ressource |

Sur l'historique `552100554?date=2025-01-01`, la réponse était HTTP 200 mais le `updatedAt` racine restait daté de 2026, comme la fiche courante. **Ne pas utiliser ce champ pour dater l'état historique demandé**, ni conclure qu'un HTTP 200 valide à lui seul la reconstitution juridique de cet état.

### 6.2 Point bloquant à qualifier : filtres documentaires

La documentation actes/comptes mélange `dateFrom` / `dateTo` dans les tableaux de paramètres et `from` / `to` dans certains exemples de pagination.

Tests réels, avec `pageSize=1` ou `2` :

| Appel | Résultat notable |
|---|---|
| `/actes?dateFrom=2026-10-20&dateTo=2026-10-21` | Un `updatedAt` retourné au **22 octobre**, hors intervalle attendu |
| `/actes?dateFrom=2024-01-01&dateTo=2024-01-02` | Un résultat daté du **1er janvier 2024** : ces paramètres ont bien un effet sur cet exemple |
| `/bilans?dateFrom=2024-01-01&dateTo=2024-01-02` | Un **tombstone** avec `updatedAt` au **14 février 2024** |
| `/bilans-saisis?dateFrom=2024-01-01&dateTo=2024-01-02` | Un **tombstone** avec `updatedAt` au **28 février 2025** |
| Les trois flux avec `from=2024-01-01&to=2024-01-02` | Résultats datés d'octobre 2026, suggérant que ces paramètres ne filtrent pas ces routes comme attendu |

**Conclusion limitée :** `dateFrom` / `dateTo` sont les paramètres à privilégier selon les tableaux documentés, mais leur comportement, notamment pour les retraits et les données récentes, n'est pas suffisamment qualifié. Ne pas affirmer qu'ils sont toujours ignorés, ni que le filtre porte parfaitement sur le `updatedAt` exposé. La cause serveur n'a pas été identifiée.

Avant une ingestion fiable : tests de bornes, cas sans résultat, données modifiées/supprimées et confirmations INPI. **Ne pas jeter automatiquement les tombstones dont le `updatedAt` est hors fenêtre** : cela pourrait laisser des documents retirés dans notre stockage.

Pour `/companies/diff`, la v5 documente `from` exclusif et `to` inclusif, avec datetime et décalage UTC. Un créneau d'une heure a renvoyé un horodatage à l'intérieur de l'intervalle après conversion du fuseau. Utiliser un encodeur de paramètres : le `+` du fuseau doit être encodé en `%2B`. Les bornes exactes et les transitions de jour restent à tester systématiquement.

### 6.3 Checksums : disponibles où ?

| Mécanisme | Observé / documenté | Ce qu'il garantit ou non |
|---|---|---|
| `ETag`, `Last-Modified` HTTP | **Absents des réponses API échantillonnées**, y compris téléchargements | Pas de stratégie `If-None-Match` garantie à ce stade |
| Hash dans les objets JSON | Aucun champ checksum identifié dans les échantillons | `id`, `version` et `nomDocument` ne remplacent pas un hash |
| CRC32 des entrées ZIP | Observé dans les répertoires centraux | Contrôle d'intégrité non cryptographique ; contenu complet nécessaire pour vérifier |
| Fichiers `.md5` des PDF SFTP | **Documentés**, non consultés en direct | Vérification de transfert ; MD5 n'est pas une preuve cryptographique d'authenticité |
| `SIZE` / `MDTM` FTP | Support annoncé ; tailles via LIST/ZIP, dates via MDTM | Détection de changement probable, pas identité de contenu |
| SHA-256 calculé par le client | Observé pour des réponses complètes et un acte PDF | Empreinte des octets reçus ; ne prouve pas l'authenticité juridique |

Exemple réel : acte `63eeeb61b40c7fba92199219`, **324 307 octets**, signature `%PDF-`, SHA-256 :

```text
9790661d7ce98342c54a337b38aa3a784d7985b476295e449cdf0354882b55dd
```

Le bilan PDF interrogé annonçait **12 635 296 octets**. Lecture arrêtée volontairement à 2 000 001 octets ; **aucun hash complet ni validation d'intégrité de ce bilan n'est revendiqué**.

Pour les futurs JSON, distinguer une empreinte des octets bruts d'une éventuelle empreinte métier/canonique. Un changement de sérialisation peut modifier le SHA-256 sans changement juridique. Ne jamais marquer un téléchargement partiel comme complet.

## 7. SFTP : organisation documentaire annoncée

Les documentations SFTP v1 de mars 2023 décrivent :

```text
Actes_PDF/AAAA/MM/JJ/<répertoire acte>/
    <acte>.pdf
    <acte>.md5
    Archive.zip

Bilans_PDF/AAAA/MM/JJ/<répertoire bilan>/
    <bilan>.PDF
    <bilan>.md5
    archive.zip
```

Pour les bilans, nommage documenté :

```text
CA_SIREN_GREFFE_GESTION_ANNEECLOTURE_NUMCHRONO
```

Un ZIP rassemble le PDF et son MD5 ; ne pas télécharger à la fois le PDF et son archive par défaut. Les documents annoncent une limite de **10 Go/jour/compte**. **Arborescence, disponibilité, quota effectif et fichiers MD5 non vérifiés en direct** pendant cette session.

Un parcours limité à « la date d'aujourd'hui » serait fragile : les répertoires sont décrits par date de dépôt, pas nécessairement date de première disponibilité. Le suivi des retraits/corrections doit aussi rester explicite.

## 8. Limites, droits et exploitation responsable

- Documentation API actes/comptes : HTTP `429` lorsque les quotas journaliers **10 000 appels API/IHM ou 10 000 Mo** sont dépassés. Aucun test de saturation ; pas de limite par seconde mesurée ni d'en-tête de quota identifié.
- Les usages API et interface peuvent partager un quota ; ne pas transposer automatiquement ce quota au FTP des stocks, dont les limites n'ont pas été établies.
- `401` : authentification ; `403` : habilitation ; `429` : quota ; `5xx` : incident serveur. Un `500` ne démontre pas une absence de droits.
- Les documents retirés sont signalés par `deleted: true`. La documentation demande au rediffuseur de supprimer les documents déjà téléchargés. **L'immuabilité raw n'autorise pas à conserver ou servir indéfiniment un document légalement retiré** : définir une procédure de retrait/purge et de traçabilité minimale compatible avec la licence.
- `formality.diffusionINSEE = "N"` : la documentation précise que le rediffuseur ne peut pas rediffuser les données concernées, même si le teneur du registre les expose.
- `diffusionCommerciale = false` : opposition à la prospection commerciale, distincte de la non-diffusion INSEE. Respecter les conditions de licence et le RGPD ; faire valider le périmètre de restitution.
- Ne pas reconstruire un bénéficiaire effectif depuis les seuls dirigeants ; ne pas contourner les déclarations de confidentialité.
- Couverture éditoriale annoncée : actes/statuts depuis 1993, comptes déposés depuis le 1er janvier 2017. **Cela ne garantit pas une série complète pour chaque entreprise.**
- Les estimations éditoriales de 28 millions d'actes et 1,5 million de dépôts de comptes/an, dont environ 45 % avec déclaration de confidentialité, ne sont pas des mesures de cette exploration et peuvent vieillir.

## 9. Ce qui est utile pour notre projet

### Priorité 1 — enrichissement juridique et financier, sans OCR

1. **Rapprocher SIRENE et RNE par SIREN**, en conservant les provenances et dates de chaque source. Par SIRET pour les établissements, sans assimiler ceux-ci à des sociétés.
2. Ajouter formes juridiques, objet social, activités et dates de création/cessation disponibles.
3. Exploiter les **bilans saisis** pour chiffre d'affaires, résultat, actifs, endettement et comparaisons par activité/territoire, seulement après décodage comptable et contrôle de couverture.
4. Construire une chronologie à partir des événements/formalités et dépôts, sans prétendre à un historique exhaustif lorsque la source ne l'établit pas.

### Priorité 2 — graphe justifiable

- `Entreprise → Établissement` : implantation, pas propriété.
- `Entreprise/Personne → Entreprise` avec un **rôle de représentation** explicitement identifié dans `composition.pouvoirs[]`.
- Représentants communs : piste de réseau, avec prudence sur l'homonymie des personnes physiques ; favoriser les SIREN des représentants personnes morales lorsqu'ils existent.
- `Document → Entreprise` et `Document → Événement` : provenance et navigation.
- Actionnariat : seulement si un acte le démontre explicitement, avec date, pièce et niveau de confiance. **Dirigeant ≠ actionnaire ; co-localisation ≠ contrôle.**

### Priorité 3 — PDF ciblés, puis éventuellement propriété industrielle

Lire quelques statuts et décisions pertinents peut permettre d'extraire des opérations de capital ou participations historiques. Commencer par le texte embarqué ; OCR uniquement si nécessaire, après validation du coût et du cadre de réutilisation.

Les brevets/marques pourraient apporter un indicateur d'innovation et des liens via les déposants/titulaires. Mais les onglets PI ne prouvent ni les droits du compte ni la présence d'un SIREN exploitable. Exploration séparée nécessaire ; ne pas promettre un rapprochement fiable par nom seul.

## 10. Conséquences pour une future ingestion — proposées, non implémentées

### Périmètre initial recommandé

**Petit ensemble de SIREN → fiches RNE + pièces associées + bilans structurés**, avant tout bootstrap national. Pas d'OCR, pas de Spark, pas de base graphe à ce stade.

### Provenance minimale par ressource

```text
source = inpi_rne
resource_type = company | acte | bilan_pdf | bilan_saisi | stock
source_id, siren, siret éventuel
endpoint ou chemin FTP/SFTP sans credentials
retrieved_at_utc, source_updated_at
business_dates : dépôt / clôture / effet selon ressource
content_type, byte_size, sha256, source_checksum éventuel
source_version éventuelle, deleted, confidentiality
run_id, window_from, window_to, cursor, processing_status
```

- **Bootstrap** : confirmer la couverture/date des stocks avant téléchargement ; estimer stockage, décompression et calcul. Environ **19,43 Go compressés** pour les deux archives observées, hors sorties et versions conservées. Aucun devis GCP ni autorisation de téléchargement massif n'est implicite.
- **Refresh** : fenêtres fixes sur `/companies/diff`, curseur exact ; flux documentaires seulement après qualification des anomalies du §6.
- **Checkpoints** : indépendants par famille de ressources, page acquittée après téléchargement et validation réussis ; fenêtre terminée seulement après pagination complète. Ne pas avancer sur échec ou sortie tronquée.
- **Idempotence** : conserver type + ID source et versions/empreintes ; ne pas utiliser seulement le nom du document. Dédupliquer au niveau métier séparément du stockage brut.
- **Corrections et retard** : recouvrement de fenêtres et réconciliation périodique à dimensionner ; ne pas supposer qu'un recouvrement arbitraire de quelques jours résout les anomalies observées.
- **Suppressions** : tombstones, retrait des sorties analytiques/documentaires, procédure de purge conforme ; conserver seulement les traces autorisées.
- **Erreurs** : backoff borné, `Retry-After` si fourni, renouvellement contrôlé sur expiration, pas de boucle sur `403` ; journalisation sans token ni contenu personnel complet.
- **Validation** : SIREN 9 caractères, SIRET 14, dates/fuseaux, clés optionnelles, doublons, fichiers complets, signature PDF, taille et checksum, cohérence comptable et rejets.

### Questions restant ouvertes avant industrialisation

- [ ] Confirmer la date métier, la couverture (actives/cessées, périmètre niveau 1) et la fréquence des stocks actuels.
- [ ] Qualifier les bornes et le traitement des retraits dans les trois flux documentaires ; obtenir une clarification INPI si nécessaire.
- [ ] Mesurer les tailles décompressées et le nombre réel d'enregistrements **après autorisation du bootstrap**.
- [ ] Tester des formes juridiques variées, entrepreneurs individuels, exploitations, comptes absents/confidentiels et changements de nomenclature.
- [ ] Valider le décodage des liasses sur plusieurs types de bilan, devises, exercices et codes de saisie.
- [ ] Approfondir les limites de l'API historique, les dates sans état et les erreurs serveur.
- [ ] Vérifier SFTP depuis un réseau compatible, ses habilitations, sa clé d'hôte et les MD5 réels.
- [ ] Vérifier les licences applicables et les règles de restitution avant publication d'un graphe ou d'une API.
- [ ] Explorer séparément les droits API/FTP PI si le cas d'usage est retenu.

## 11. Reproduire une vérification ciblée sans exposer les secrets

### Configuration locale

Le fichier versionnable [`.env.example`](../.env.example) fournit les variables `INPI_API_BASE_URL`, `INPI_USERNAME`, `INPI_PASSWORD`, ainsi que `INPI_FTP_*` et `INPI_SFTP_*` (hôte, port, identifiant, mot de passe). Le fichier local `.env` contient les valeurs privées, est exclu de Git et doit rester lisible uniquement par son propriétaire (`chmod 600 .env`). Ne jamais commiter ses valeurs ni y conserver un JWT. Les accès SFTP restent non validés ; leur configuration n'est pas une preuve d'accès. **Aucun chargement automatique de `.env` n'est implémenté dans le projet.**

### Vérification interactive

Exemple Python **illustratif**, non ajouté comme connecteur. Exécuter avec Python 3.13 depuis un terminal interactif ; il demande les identifiants sans les écrire dans l'historique shell. Il n'affiche ni token ni données d'entreprise.

```python
import getpass
import json
import urllib.request

base = "https://registre-national-entreprises.inpi.fr/api"
email = input("Email INPI : ")
password = getpass.getpass("Mot de passe INPI : ")
request = urllib.request.Request(
    f"{base}/sso/login",
    data=json.dumps({"username": email, "password": password}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=30) as response:
    token = json.load(response)["token"]
password = None
request = urllib.request.Request(
    f"{base}/companies/552100554",
    headers={"Authorization": f"Bearer {token}"},
)
with urllib.request.urlopen(request, timeout=30) as response:
    company = json.load(response)
    print("HTTP", response.status, "— clés racine :", sorted(company))
token = None
```

Ne pas activer de logs HTTP détaillés, copier le JWT dans une commande shell ou commiter un fichier d'identifiants. Le mot de passe ayant été transmis dans la conversation, son changement est recommandé. L'extrait n'implémente ni retries ni pagination : ce n'est pas le futur pipeline.

## 12. Périmètre exact des vérifications et références

### Vérifications exécutées

- Lecture des deux captures, de `AGENTS.md`, `README.md` et `pyproject.toml` ; branche `docs/exploration-inpi` créée.
- Trois connexions API ; **29 GET** au total, incluant les deux téléchargements PDF et une seconde vérification des deux tombstones. Pas de parcours exhaustif.
- Fiches des SIREN publics `552100554`, `542107651`, `542065479` : HTTP 200. L'une atteignait **2 309 704 octets** : une fiche peut déjà être volumineuse.
- Schémas inférés sans conserver les valeurs personnelles : au plus huit éléments inspectés par tableau dans les réponses API, deux dans le premier objet de chaque stock. **Ce ne sont pas des schémas exhaustifs ou contractuels.**
- Connexion FTP et échantillons ZIP bornés ; pas de téléchargement national.
- Une tentative réseau SFTP : timeout, sans authentification ni liste distante obtenue.
- Téléchargement de quatre documentations PDF publiques (formalités, comptes, actes API, actes SFTP) et extraction par `pdftotext` ; documentation SFTP comptes consultée via l'index web.
- Rapports temporaires limités aux schémas, métadonnées techniques, empreintes et listes de fichiers, hors dépôt ; aucun token sauvegardé, aucun original d'entreprise ajouté à Git.
- Utilisation de Python 3.13 via `uv run --no-project` ; Paramiko fourni temporairement pour le test réseau SFTP. Aucune dépendance ajoutée au projet.
- **Aucune opération GCP, aucun upload, aucun OCR, aucun déploiement ni changement IAM.** Pas de tests unitaires de connecteur : aucun connecteur n'a été implémenté.

### Sources officielles consultées

Les versions ci-dessous sont celles effectivement consultées ; elles peuvent être remplacées par l'INPI.

1. [Catalogue API Entreprises — DATA INPI](https://data.inpi.fr/content/editorial/Acces_API_Entreprises) — familles de données, couverture, fréquences et estimations.
2. [Catalogue FTP/SFTP Entreprises — DATA INPI](https://data.inpi.fr/content/editorial/Serveur_ftp_entreprises) — accès et catalogue ; attention aux références historiques de stock.
3. [Accès à l'API formalité / RNE — INPI](https://www.inpi.fr/ressources/formalites-dentreprises/acces-lapi-formalite-rne).
4. [Documentation API formalités **v5.0, août 2026**](https://www.inpi.fr/sites/default/files/2026-08/documentation%20technique%20API%20formalite%CC%81s_v5.0-AA.pdf) — endpoints, datetime différentielle, droits de diffusion ; **annexe 1 : dictionnaire détaillé des blocs publics**, annexes 2/3 : exemples.
5. [Documentation API comptes annuels **v6.0, septembre 2026**](https://www.inpi.fr/sites/default/files/2026-09/documentation%20technique%20API_comptes_annuels%20v6-1-AA.pdf) — structure, tables de liasses, devises/contexte, codes de saisie, confidentialité, retraits, quotas.
6. [Documentation API actes **v4.0, juin 2025**](https://www.inpi.fr/sites/default/files/2025-06/documentation%20technique%20API%20Actes%20v4.0.pdf) — métadonnées, téléchargement et flux différentiel ; incohérence des exemples de filtres signalée plus haut.
7. [Documentation SFTP actes **v1.0, mars 2023**](https://www.inpi.fr/sites/default/files/Documentation%20technique%20SFTP%20Actes%20v1.0%20mars%202023_0.pdf) — port, arborescence, MD5, quota annoncé.
8. [Documentation SFTP comptes annuels **v1.0, mars 2023**](https://www.inpi.fr/sites/default/files/Documentation%20technique%20SFTP%20Comptes-annuels%20v1.0%20mars%202023_0.pdf) — nommage et organisation documentaire.

Le catalogue API propose également un dictionnaire des données et une table des catégories d'activités. Ces fichiers n'ont pas été téléchargés ni versionnés pendant cette session ; les récupérer depuis le portail avant de figer un schéma de production. Ne pas présenter les arbres simplifiés ci-dessus comme « tous les champs possibles de DATA INPI ».
