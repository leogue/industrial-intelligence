"""Spark ETL (Serverless for Apache Spark): bronze SIRENE, RNE, BODACC -> Iceberg silver and gold.

Silver normalises each source, keyed on `siren` / `siret`; `liens` holds every relation between
companies, people, establishments as one edge list. Gold serves the first product queries
(company page, company network, territory indicators) without re-joining the national tables.

The file only imports pyspark and the standard library: Dataproc runs it as the batch main file.
"""

import argparse
import json
import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Above this many mandates, a person or company is a professional (auditor, manager, holding
# operator): pairing all its companies would add millions of edges that say nothing about groups.
MAX_SHARED_MANDATES = 50

# Only the RNE fields used downstream: from_json skips the rest of the (large) company document.
# Scalars of uncertain JSON type are read as strings, which Spark accepts for any JSON scalar.
_PERSON = "STRUCT<nom: STRING, nomUsage: STRING, prenoms: ARRAY<STRING>, dateDeNaissance: STRING>"
_COMPANY = (
    "STRUCT<denomination: STRING, formeJuridique: STRING, codeApe: STRING, dateImmat: STRING,"
    " siren: STRING, pays: STRING>"
)
_POUVOIR = (
    "STRUCT<representantId: STRING, roleEntreprise: STRING, typeDePersonne: STRING,"
    f" actif: BOOLEAN, individu: STRUCT<descriptionPersonne: {_PERSON}>, entreprise: {_COMPANY}>"
)
_DESCRIPTION = (
    "STRUCT<montantCapital: DOUBLE, deviseCapital: STRING, capitalVariable: STRING,"
    " dateClotureExerciceSocial: STRING, objet: STRING>"
)
RNE_SCHEMA = (
    "formality STRUCT<content: STRUCT<"
    f"personneMorale: STRUCT<identite: STRUCT<entreprise: {_COMPANY}, description: {_DESCRIPTION}>,"
    f" composition: STRUCT<pouvoirs: ARRAY<{_POUVOIR}>>>,"
    f" personnePhysique: STRUCT<identite: STRUCT<entreprise: {_COMPANY},"
    f" entrepreneur: STRUCT<descriptionPersonne: {_PERSON}>>>>>"
)

LIEN_COLUMNS = [
    "type_lien",
    "de_type",
    "de_id",
    "vers_type",
    "vers_id",
    "date_lien",
    "role_code",
    "preuve",
    "source",
    "reference",
]


# ---------------------------------------------------------------------------------------------
# Silver


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim strings; '' and the INSEE '[ND]' (not disclosed) become null, nothing is guessed."""
    columns = []
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            value = F.trim(F.col(field.name))
            columns.append(F.when(value.isin("", "[ND]"), None).otherwise(value).alias(field.name))
        else:
            columns.append(F.col(field.name))
    return df.select(columns)


def departement(code_commune):
    # Overseas communes use a three-character department (971...976); Corsica keeps 2A/2B.
    return F.when(code_commune.startswith("97"), F.substring(code_commune, 1, 3)).otherwise(
        F.substring(code_commune, 1, 2)
    )


def etablissements(etab: DataFrame) -> DataFrame:
    etab = clean_strings(etab)
    return etab.select(
        "siret",
        "siren",
        "nic",
        F.col("etablissementSiege").alias("siege"),
        F.col("etatAdministratifEtablissement").alias("etat_administratif"),
        F.col("dateCreationEtablissement").alias("date_creation"),
        F.col("dateDebut").alias("date_debut_etat"),
        F.col("activitePrincipaleEtablissement").alias("activite_principale"),
        F.col("trancheEffectifsEtablissement").alias("tranche_effectifs"),
        F.col("caractereEmployeurEtablissement").alias("employeur"),
        F.coalesce("enseigne1Etablissement", "denominationUsuelleEtablissement").alias("enseigne"),
        F.concat_ws(
            " ",
            "numeroVoieEtablissement",
            "indiceRepetitionEtablissement",
            "typeVoieEtablissement",
            "libelleVoieEtablissement",
        ).alias("adresse"),
        F.col("codePostalEtablissement").alias("code_postal"),
        F.col("libelleCommuneEtablissement").alias("commune"),
        F.col("codeCommuneEtablissement").alias("code_commune"),
        departement(F.col("codeCommuneEtablissement")).alias("departement"),
        F.col("statutDiffusionEtablissement").alias("statut_diffusion"),
    )


def rne_current(rne: DataFrame) -> DataFrame:
    """Latest RNE record per SIREN (stock + finished diff windows), JSON parsed before the shuffle.

    Same ordering as the Silver prototype: technical update time, then retrieval time, diff
    before stock on ties, then the content hash so reruns pick the same row.
    """
    parsed = rne.select(
        "siren",
        "source_updated_at",
        "source_retrieved_at",
        "source_kind",
        "company_sha256",
        "type_personne",
        "denomination",
        "forme_juridique",
        "code_ape",
        "diffusion_commerciale",
        "deleted",
        F.from_json("company_json", RNE_SCHEMA).alias("doc"),
    )
    order = Window.partitionBy("siren").orderBy(
        F.col("source_updated_at").desc(),
        F.col("source_retrieved_at").desc(),
        (F.col("source_kind") == "diff").desc(),
        F.col("company_sha256").desc(),
    )
    return parsed.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").drop("_rank")


def entreprises(ul: DataFrame, rne: DataFrame, etabs: DataFrame) -> DataFrame:
    """One row per SIREN known to SIRENE or the RNE, both sources kept side by side."""
    ul = clean_strings(ul)
    sirene = ul.select(
        "siren",
        F.coalesce(
            "denominationUniteLegale",
            F.concat_ws(
                " ",
                F.coalesce("prenomUsuelUniteLegale", "prenom1UniteLegale"),
                F.coalesce("nomUsageUniteLegale", "nomUniteLegale"),
            ),
        ).alias("denomination"),
        F.col("sigleUniteLegale").alias("sigle"),
        F.col("categorieJuridiqueUniteLegale").cast("string").alias("categorie_juridique"),
        F.col("activitePrincipaleUniteLegale").alias("activite_principale"),
        F.col("activitePrincipaleNAF25UniteLegale").alias("activite_principale_naf25"),
        F.col("trancheEffectifsUniteLegale").alias("tranche_effectifs"),
        F.col("anneeEffectifsUniteLegale").alias("annee_effectifs"),
        F.col("categorieEntreprise").alias("categorie_entreprise"),
        F.col("etatAdministratifUniteLegale").alias("etat_administratif"),
        F.col("dateCreationUniteLegale").alias("date_creation"),
        F.col("dateDebut").alias("date_debut_etat"),
        F.col("economieSocialeSolidaireUniteLegale").alias("ess"),
        F.col("societeMissionUniteLegale").alias("societe_mission"),
        F.col("caractereEmployeurUniteLegale").alias("employeur"),
        F.col("statutDiffusionUniteLegale").alias("statut_diffusion"),
        F.concat("siren", "nicSiegeUniteLegale").alias("siret_siege"),
        F.lit(True).alias("dans_sirene"),
    )
    description = "doc.formality.content.personneMorale.identite.description"
    pm = "doc.formality.content.personneMorale.identite.entreprise"
    pp = "doc.formality.content.personnePhysique.identite.entreprise"
    registre = rne.select(
        "siren",
        F.col("denomination").alias("rne_denomination"),
        F.col("type_personne").alias("rne_type_personne"),
        F.col("forme_juridique").alias("rne_forme_juridique"),
        F.col("code_ape").alias("rne_code_ape"),
        F.coalesce(f"{pm}.dateImmat", f"{pp}.dateImmat").alias("date_immatriculation"),
        F.col(f"{description}.montantCapital").alias("capital"),
        F.col(f"{description}.deviseCapital").alias("devise_capital"),
        F.col(f"{description}.capitalVariable").alias("capital_variable"),
        F.col(f"{description}.dateClotureExerciceSocial").alias("date_cloture_exercice"),
        F.col(f"{description}.objet").alias("objet_social"),
        F.col("diffusion_commerciale").alias("rne_diffusion_commerciale"),
        F.col("deleted").alias("rne_supprimee"),
        F.col("source_kind").alias("rne_source"),
        F.col("source_updated_at").alias("rne_mis_a_jour_le"),
        F.lit(True).alias("dans_rne"),
    )
    siege = etabs.where("siege").select(
        F.col("siret").alias("siret_siege"),
        F.col("adresse").alias("siege_adresse"),
        F.col("code_postal").alias("siege_code_postal"),
        F.col("commune").alias("siege_commune"),
        F.col("code_commune").alias("siege_code_commune"),
        F.col("departement").alias("siege_departement"),
    )
    counts = etabs.groupBy("siren").agg(
        F.count("*").alias("nb_etablissements"),
        F.count(F.when(F.col("etat_administratif") == "A", True)).alias("nb_etablissements_actifs"),
    )
    return (
        sirene.join(registre, "siren", "full")
        .join(siege, "siret_siege", "left")
        .join(counts, "siren", "left")
        .select(
            "siren",
            F.coalesce("denomination", "rne_denomination").alias("denomination"),
            *[c for c in sirene.columns if c not in {"siren", "denomination", "dans_sirene"}],
            *[c for c in registre.columns if c not in {"siren", "dans_rne"}],
            *[c for c in siege.columns if c != "siret_siege"],
            F.coalesce("nb_etablissements", F.lit(0)).alias("nb_etablissements"),
            F.coalesce("nb_etablissements_actifs", F.lit(0)).alias("nb_etablissements_actifs"),
            F.coalesce("dans_sirene", F.lit(False)).alias("dans_sirene"),
            F.coalesce("dans_rne", F.lit(False)).alias("dans_rne"),
        )
    )


def digits(column):
    """A SIREN written with spaces ('552 100 554'); anything that is not 9 digits becomes null."""
    value = F.regexp_replace(column, r"\s", "")
    return F.when(value.rlike("^[0-9]{9}$"), value)


def person_key(nom, prenoms, naissance):
    # Same name, first names and birth month: an inference (namesakes exist), never a fact.
    parts = [F.upper(F.trim(nom)), F.upper(F.concat_ws(" ", prenoms)), naissance]
    complete = nom.isNotNull() & naissance.isNotNull() & (F.size(prenoms) > 0)
    return F.when(complete, F.sha2(F.concat_ws("|", *parts), 256))


def dirigeants(rne: DataFrame) -> DataFrame:
    """Legal representatives (RNE `composition.pouvoirs`) and individual entrepreneurs.

    A representative is not a shareholder: nothing here says who owns the company.
    """
    pouvoir = F.col("p")
    person = pouvoir["individu"]["descriptionPersonne"]
    company = pouvoir["entreprise"]
    pouvoirs = rne.select(
        "siren",
        F.explode("doc.formality.content.personneMorale.composition.pouvoirs").alias("p"),
    ).select(
        "siren",
        F.lit("pouvoir").alias("nature"),
        pouvoir["representantId"].alias("representant_id"),
        pouvoir["roleEntreprise"].alias("role_code"),
        pouvoir["typeDePersonne"].alias("type_personne"),
        pouvoir["actif"].alias("actif"),
        person["nom"].alias("nom"),
        person["nomUsage"].alias("nom_usage"),
        F.concat_ws(" ", person["prenoms"]).alias("prenoms"),
        person["dateDeNaissance"].alias("mois_naissance"),
        person_key(person["nom"], person["prenoms"], person["dateDeNaissance"]).alias(
            "personne_id"
        ),
        digits(company["siren"]).alias("siren_dirigeant"),
        company["denomination"].alias("denomination_dirigeant"),
        company["formeJuridique"].alias("forme_juridique_dirigeant"),
        company["pays"].alias("pays_dirigeant"),
    )
    entrepreneur = F.col("doc.formality.content.personnePhysique.identite.entrepreneur")
    person = entrepreneur["descriptionPersonne"]
    individuels = rne.where(entrepreneur.isNotNull()).select(
        "siren",
        F.lit("entrepreneur_individuel").alias("nature"),
        F.lit(None).cast("string").alias("representant_id"),
        F.lit(None).cast("string").alias("role_code"),
        F.lit("INDIVIDU").alias("type_personne"),
        F.lit(None).cast("boolean").alias("actif"),
        person["nom"].alias("nom"),
        person["nomUsage"].alias("nom_usage"),
        F.concat_ws(" ", person["prenoms"]).alias("prenoms"),
        person["dateDeNaissance"].alias("mois_naissance"),
        person_key(person["nom"], person["prenoms"], person["dateDeNaissance"]).alias(
            "personne_id"
        ),
        *[
            F.lit(None).cast("string").alias(name)
            for name in (
                "siren_dirigeant",
                "denomination_dirigeant",
                "forme_juridique_dirigeant",
                "pays_dirigeant",
            )
        ],
    )
    return pouvoirs.unionByName(individuels)


def latest_by_id(bodacc: DataFrame) -> DataFrame:
    """A day exported twice keeps, per notice id, the copy from the most recent file."""
    order = Window.partitionBy("id").orderBy(F.col("_modified").desc(), F.col("_file").desc())
    return bodacc.withColumn("_rank", F.row_number().over(order)).where("_rank = 1")


def annonces(bodacc: DataFrame) -> DataFrame:
    """BODACC notices. SIRENs come from the `registre` field only, never from free text."""
    tokens = F.transform(F.split("registre", ","), lambda x: F.regexp_replace(x, r"\s", ""))
    sirens = F.array_distinct(F.filter(tokens, lambda x: x.rlike("^[0-9]{9}$")))
    # Previous owners (sale of a business): the immatriculation numbers in their JSON block.
    sellers = F.array_distinct(
        F.transform(
            F.regexp_extract_all(
                "listeprecedentproprietaire", F.lit(r'"numeroIdentification"\s*:\s*"([0-9 ]+)"'), 1
            ),
            lambda x: F.regexp_replace(x, r"\s", ""),
        )
    )
    jugement = F.from_json(
        "jugement", "famille STRING, nature STRING, date STRING, complementJugement STRING"
    )
    depot = F.from_json("depot", "dateCloture STRING, typeDepot STRING")
    return latest_by_id(bodacc).select(
        "id",
        "dateparution",
        F.col("numeroannonce").alias("numero_annonce"),
        F.col("familleavis").alias("famille"),
        F.col("familleavis_lib").alias("famille_libelle"),
        F.col("typeavis").alias("type_avis"),
        "tribunal",
        F.col("numerodepartement").alias("departement"),
        "ville",
        "cp",
        "commercant",
        sirens.alias("sirens"),
        F.filter(sellers, lambda x: x.rlike("^[0-9]{9}$")).alias("sirens_cedants"),
        jugement["famille"].alias("jugement_famille"),
        jugement["nature"].alias("jugement_nature"),
        jugement["date"].alias("jugement_date"),
        depot["dateCloture"].alias("depot_date_cloture"),
        depot["typeDepot"].alias("depot_type"),
        F.col("url_complete").alias("url"),
        F.col("_file").alias("source_fichier"),
    )


def liens(
    dirs: DataFrame, successions: DataFrame, doublons: DataFrame, notices: DataFrame
) -> DataFrame:
    """Every relation as one edge: (de_type, de_id) -> (vers_type, vers_id).

    `preuve` says what the edge rests on: `fait` is stated by the source as such;
    `inference` depends on our person matching. No edge is an ownership link.
    """
    none = F.lit(None).cast("string")
    mandats_pm = dirs.where("siren_dirigeant IS NOT NULL").select(
        F.lit("mandat_personne_morale").alias("type_lien"),
        F.lit("entreprise").alias("de_type"),
        F.col("siren_dirigeant").alias("de_id"),
        F.lit("entreprise").alias("vers_type"),
        F.col("siren").alias("vers_id"),
        none.alias("date_lien"),
        "role_code",
        F.lit("fait").alias("preuve"),
        F.lit("rne").alias("source"),
        F.col("representant_id").alias("reference"),
    )
    mandats_pp = dirs.where("personne_id IS NOT NULL").select(
        F.when(F.col("nature") == "pouvoir", "mandat_personne_physique")
        .otherwise("entrepreneur_individuel")
        .alias("type_lien"),
        F.lit("personne").alias("de_type"),
        F.col("personne_id").alias("de_id"),
        F.lit("entreprise").alias("vers_type"),
        F.col("siren").alias("vers_id"),
        none.alias("date_lien"),
        "role_code",
        F.lit("inference").alias("preuve"),
        F.lit("rne").alias("source"),
        F.col("representant_id").alias("reference"),
    )
    succ = clean_strings(successions)
    etab_succ = succ.select(
        F.lit("succession_etablissement").alias("type_lien"),
        F.lit("etablissement").alias("de_type"),
        F.col("siretEtablissementPredecesseur").alias("de_id"),
        F.lit("etablissement").alias("vers_type"),
        F.col("siretEtablissementSuccesseur").alias("vers_id"),
        F.col("dateLienSuccession").cast("string").alias("date_lien"),
        none.alias("role_code"),
        F.lit("fait").alias("preuve"),
        F.lit("sirene").alias("source"),
        F.when(F.col("transfertSiege"), "transfert_siege").alias("reference"),
    )
    # The company-level edge only aggregates the establishment fact: same proof level.
    entreprise_succ = (
        etab_succ.select(
            F.lit("succession_entre_entreprises").alias("type_lien"),
            F.lit("entreprise").alias("de_type"),
            F.substring("de_id", 1, 9).alias("de_id"),
            F.lit("entreprise").alias("vers_type"),
            F.substring("vers_id", 1, 9).alias("vers_id"),
            "date_lien",
            "role_code",
            "preuve",
            "source",
            "reference",
        )
        .where("de_id <> vers_id")
        .groupBy("type_lien", "de_type", "de_id", "vers_type", "vers_id")
        .agg(
            F.min("date_lien").alias("date_lien"),
            F.first("role_code").alias("role_code"),
            F.first("preuve").alias("preuve"),
            F.first("source").alias("source"),
            F.lit(None).cast("string").alias("reference"),
        )
    )
    doubles = clean_strings(doublons).select(
        F.lit("doublon_siren").alias("type_lien"),
        F.lit("entreprise").alias("de_type"),
        F.col("siren").alias("de_id"),
        F.lit("entreprise").alias("vers_type"),
        F.col("sirenDoublon").alias("vers_id"),
        none.alias("date_lien"),
        none.alias("role_code"),
        F.lit("fait").alias("preuve"),
        F.lit("sirene").alias("source"),
        none.alias("reference"),
    )
    # Sale of a business (fonds de commerce): seller -> buyer, as published in the notice.
    # Buyers are the notice's other registered SIRENs; it transfers an asset, not shares.
    ventes = (
        notices.where("famille = 'vente' AND size(sirens_cedants) > 0")
        .select(
            "id",
            "dateparution",
            F.explode("sirens_cedants").alias("cedant"),
            F.array_except("sirens", "sirens_cedants").alias("acquereurs"),
        )
        .select("*", F.explode("acquereurs").alias("acquereur"))
        .select(
            F.lit("cession_fonds").alias("type_lien"),
            F.lit("entreprise").alias("de_type"),
            F.col("cedant").alias("de_id"),
            F.lit("entreprise").alias("vers_type"),
            F.col("acquereur").alias("vers_id"),
            F.col("dateparution").cast("string").alias("date_lien"),
            none.alias("role_code"),
            F.lit("fait").alias("preuve"),
            F.lit("bodacc").alias("source"),
            F.col("id").alias("reference"),
        )
    )
    edges = mandats_pm
    for other in (mandats_pp, etab_succ, entreprise_succ, doubles, ventes):
        edges = edges.unionByName(other)
    return edges.select(LIEN_COLUMNS)


# ---------------------------------------------------------------------------------------------
# Gold


def fiche_entreprise(companies: DataFrame, dirs: DataFrame, notices: DataFrame) -> DataFrame:
    """The company page: identity, head office, representatives and BODACC signals in one row."""
    representants = (
        dirs.where("actif IS NULL OR actif")
        .groupBy("siren")
        .agg(
            F.count("*").alias("nb_dirigeants"),
            F.collect_list(
                F.struct(
                    "role_code",
                    "type_personne",
                    "nom",
                    "prenoms",
                    "siren_dirigeant",
                    "denomination_dirigeant",
                )
            ).alias("dirigeants"),
        )
    )
    by_company = notices.select("*", F.explode("sirens").alias("siren"))
    signaux = by_company.groupBy("siren").agg(
        F.count("*").alias("nb_annonces_bodacc"),
        F.max("dateparution").alias("derniere_annonce"),
        F.max(F.when(F.col("famille") == "collective", F.col("dateparution"))).alias(
            "derniere_procedure_collective"
        ),
        # max() skips nulls: the struct of the latest collective-procedure notice.
        F.max(
            F.when(F.col("famille") == "collective", F.struct("dateparution", "jugement_nature"))
        )["jugement_nature"].alias("dernier_jugement"),
        F.max(F.when(F.col("famille") == "dpc", F.col("depot_date_cloture"))).alias(
            "dernier_exercice_depose"
        ),
    )
    return companies.join(representants, "siren", "left").join(signaux, "siren", "left")


def reseau_entreprises(edges: DataFrame, companies: DataFrame) -> DataFrame:
    """Company-to-company edges, direct or through a shared representative, with both names."""
    direct = edges.where("de_type = 'entreprise' AND vers_type = 'entreprise'").select(
        F.col("de_id").alias("siren_a"),
        F.col("vers_id").alias("siren_b"),
        "type_lien",
        "preuve",
        "date_lien",
        "reference",
    )
    # Two companies sharing a representative (a person or a company): a hint of a group.
    mandats = (
        edges.where(F.col("type_lien").isin("mandat_personne_morale", "mandat_personne_physique"))
        .select("de_type", "de_id", F.col("vers_id").alias("siren"))
        .distinct()
    )
    sizes = mandats.groupBy("de_type", "de_id").agg(F.count("*").alias("n"))
    small = mandats.join(sizes.where(f"n <= {MAX_SHARED_MANDATES}"), ["de_type", "de_id"])
    left, right = small.alias("l"), small.alias("r")
    partage = (
        left.join(
            right,
            (F.col("l.de_type") == F.col("r.de_type"))
            & (F.col("l.de_id") == F.col("r.de_id"))
            & (F.col("l.siren") < F.col("r.siren")),
        )
        .select(
            F.col("l.siren").alias("siren_a"),
            F.col("r.siren").alias("siren_b"),
            F.when(F.col("l.de_type") == "personne", "dirigeant_commun")
            .otherwise("personne_morale_commune")
            .alias("type_lien"),
            F.when(F.col("l.de_type") == "personne", "inference").otherwise("fait").alias("preuve"),
            F.lit(None).cast("string").alias("date_lien"),
            F.col("l.de_id").alias("reference"),
        )
        .distinct()
    )
    names = companies.select("siren", "denomination")
    return (
        direct.unionByName(partage)
        .join(names.toDF("siren_a", "denomination_a"), "siren_a", "left")
        .join(names.toDF("siren_b", "denomination_b"), "siren_b", "left")
        .select(
            "siren_a",
            "denomination_a",
            "siren_b",
            "denomination_b",
            "type_lien",
            "preuve",
            "date_lien",
            "reference",
        )
    )


def indicateurs_territoire(companies: DataFrame, notices: DataFrame) -> DataFrame:
    """Monthly creations and BODACC events per department and NAF division (2 digits)."""
    secteur = companies.select(
        "siren", F.substring("activite_principale", 1, 2).alias("division_naf")
    )
    # 1900-01-01 stands for an unknown date (10 % of companies in the 26/11 sample, against
    # a handful for any other day): kept as supplied in silver, not counted as a creation.
    creations = companies.where(
        "date_creation IS NOT NULL AND date_creation <> DATE '1900-01-01'"
    ).select(
        F.trunc("date_creation", "month").alias("mois"),
        F.col("siege_departement").alias("departement"),
        F.substring("activite_principale", 1, 2).alias("division_naf"),
        F.lit("creation").alias("evenement"),
    )
    evenements = (
        notices.where(F.col("famille").isin("collective", "radiation", "vente", "creation"))
        .select(
            F.trunc("dateparution", "month").alias("mois"),
            "departement",
            F.col("famille").alias("evenement"),
            F.element_at("sirens", 1).alias("siren"),
        )
        .join(secteur, "siren", "left")
        .select(
            "mois",
            "departement",
            "division_naf",
            F.concat(F.lit("bodacc_"), "evenement").alias("evenement"),
        )
    )
    return (
        creations.unionByName(evenements)
        .groupBy("mois", "departement", "division_naf")
        .pivot(
            "evenement",
            [
                "creation",
                "bodacc_creation",
                "bodacc_collective",
                "bodacc_radiation",
                "bodacc_vente",
            ],
        )
        .count()
        .na.fill(0)
        .withColumnRenamed("creation", "creations_sirene")
        .withColumnRenamed("bodacc_creation", "creations_bodacc")
        .withColumnRenamed("bodacc_collective", "procedures_collectives")
        .withColumnRenamed("bodacc_radiation", "radiations")
        .withColumnRenamed("bodacc_vente", "ventes_fonds")
    )


# ---------------------------------------------------------------------------------------------
# Validation and I/O


def require_identifiers(df: DataFrame, table: str, widths: dict[str, int]) -> None:
    """Bronze is external input: a malformed SIREN/SIRET stops the run before any write."""
    bad = F.lit(False)
    for column, width in widths.items():
        bad = bad | F.col(column).isNull() | ~F.col(column).rlike(f"^[0-9]{{{width}}}$")
    if df.where(bad).limit(1).count():
        raise ValueError(f"{table}: invalid identifier, run stopped before writing")


def hadoop_path(spark: SparkSession, uri: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path, path.getFileSystem(spark._jsc.hadoopConfiguration())


def list_dirs(spark: SparkSession, uri: str) -> list[str]:
    path, fs = hadoop_path(spark, uri)
    return sorted(s.getPath().getName() for s in fs.listStatus(path) if s.isDirectory())


def latest_partition(spark: SparkSession, uri: str) -> str:
    dates = [name for name in list_dirs(spark, uri) if name.startswith("date=")]
    if not dates:
        raise ValueError(f"No date= partition under {uri}")
    return f"{uri}/{dates[-1]}"


def read_json(spark: SparkSession, uri: str) -> dict:
    # Through the Hadoop API: Spark readers skip files whose name starts with "_".
    path, fs = hadoop_path(spark, uri)
    stream = fs.open(path)
    try:
        text = spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()
    return json.loads(text)


def parquet_files(spark: SparkSession, uri: str, name: str) -> DataFrame:
    """Parquet files named `name` anywhere under `uri`, without inferring partition columns.

    The folders also hold JSON manifests and rejects; RNE diff folders contain ':', which
    Hadoop glob patterns reject, hence a recursive listing filtered on the file name.
    """
    return (
        spark.read.option("recursiveFileLookup", "true").option("pathGlobFilter", name).parquet(uri)
    )


RNE_COLUMNS = [
    "siren",
    "source_updated_at",
    "source_retrieved_at",
    "source_kind",
    "company_sha256",
    "type_personne",
    "denomination",
    "forme_juridique",
    "code_ape",
    "diffusion_commerciale",
    "deleted",
    "company_json",
]


def read_inputs(spark: SparkSession, root: str, rne_stock: str) -> tuple[dict, dict]:
    sirene = f"{root}/bronze/insee/sirene"
    paths = {
        name: latest_partition(spark, f"{sirene}/{name}")
        for name in ("unite_legale", "etablissement", "etablissement_liens_succession", "doublons")
    }
    stock = f"{root}/bronze/inpi/rne/companies/stock/{rne_stock}"
    manifest = read_json(spark, f"{stock}/_manifest.json")
    if manifest.get("status") != "complete":
        raise ValueError("RNE stock snapshot is not complete")
    rne_until = read_json(spark, f"{root}/control/inpi/rne/diff/checkpoint.json")["completed_until"]
    bodacc_until = read_json(spark, f"{root}/control/dila/bodacc/checkpoint.json")[
        "completed_until"
    ]
    frames = {name: parquet_files(spark, path, "*.parquet") for name, path in paths.items()}
    stock_rows = parquet_files(spark, stock, "part-*.parquet").select(RNE_COLUMNS)
    # Only diff windows the checkpoint has passed: the open window may be partially written.
    # Window bounds are UTC ISO 8601 strings in the path, which compare lexically.
    diff = parquet_files(spark, f"{root}/bronze/inpi/rne/companies/diff", "companies.parquet")
    window_end = F.regexp_extract("_metadata.file_path", r"/to=([^/]+)/", 1)
    frames["rne"] = stock_rows.unionByName(diff.where(window_end <= rne_until).select(RNE_COLUMNS))
    bodacc = parquet_files(spark, f"{root}/raw/dila/bodacc/annonces", "*.parquet")
    frames["bodacc"] = bodacc.where(
        F.col("dateparution") <= F.lit(bodacc_until).cast("date")
    ).select(
        "*",
        F.col("_metadata.file_path").alias("_file"),
        F.col("_metadata.file_modification_time").alias("_modified"),
    )
    provenance = {
        **paths,
        "rne_stock": stock,
        "rne_stock_rows": manifest.get("valid_rows"),
        "rne_diff_until": rne_until,
        "bodacc_until": bodacc_until,
    }
    return frames, provenance


def sample(frames: dict, prefix: str) -> dict:
    """Keep the companies whose SIREN starts with `prefix` in every source: joins stay coherent."""
    keep = F.col("siren").startswith(prefix)
    out = {
        name: frames[name].where(keep)
        for name in ("unite_legale", "etablissement", "doublons", "rne")
    }
    out["etablissement_liens_succession"] = frames["etablissement_liens_succession"].where(
        F.col("siretEtablissementPredecesseur").startswith(prefix)
    )
    # A registry token is a SIREN, written compact or spaced ("300010378,300 010 378").
    out["bodacc"] = frames["bodacc"].where(F.col("registre").rlike(f"(^|,)\\s*{prefix}"))
    return out


def write(df: DataFrame, table: str, keys: list[str], properties: dict[str, str]) -> None:
    # Range-partitioned then sorted on the lookup key: Parquet min/max stats skip most files
    # on `WHERE siren = ...`. createOrReplace is one Iceberg commit; history stays readable.
    writer = (
        df.repartitionByRange(*keys)
        .sortWithinPartitions(*keys)
        .writeTo(table)
        .using("iceberg")
        .tableProperty("write.parquet.compression-codec", "zstd")
    )
    for key, value in properties.items():
        writer = writer.option(f"snapshot-property.{key}", value)
    writer.createOrReplace()


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    frames, provenance = read_inputs(spark, args.root, args.rne_stock)
    if args.siren_prefix:
        frames = sample(frames, args.siren_prefix)
    require_identifiers(frames["unite_legale"], "unite_legale", {"siren": 9})
    require_identifiers(frames["etablissement"], "etablissement", {"siren": 9, "siret": 14})

    etabs = etablissements(frames["etablissement"]).persist()
    if etabs.where(F.substring("siret", 1, 9) != F.col("siren")).limit(1).count():
        raise ValueError("etablissement: SIRET does not belong to its SIREN")
    rne = rne_current(frames["rne"]).persist()
    companies = entreprises(frames["unite_legale"], rne, etabs)
    dirs = dirigeants(rne)
    notices = annonces(frames["bodacc"])

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    properties = {
        "etl-run": run_id,
        "etl-inputs": json.dumps(provenance, sort_keys=True),
        "etl-sample": args.siren_prefix or "",
    }
    silver, gold = f"{args.catalog}.{args.silver}", f"{args.catalog}.{args.gold}"
    tables = [
        (f"{silver}.entreprises", companies, ["siren"]),
        (f"{silver}.etablissements", etabs, ["siren", "siret"]),
        (f"{silver}.dirigeants", dirs, ["siren"]),
        (f"{silver}.annonces_bodacc", notices, ["dateparution", "id"]),
    ]
    for table, df, keys in tables:
        write(df, table, keys, properties)
    # Edges reuse the committed tables: the thousands of small BODACC files are read once.
    edges = liens(
        spark.table(f"{silver}.dirigeants"),
        frames["etablissement_liens_succession"],
        frames["doublons"],
        spark.table(f"{silver}.annonces_bodacc"),
    )
    write(edges, f"{silver}.liens", ["de_id", "vers_id"], properties)
    # Gold reads the committed silver tables instead of recomputing the lineage.
    companies, dirs, notices, edges = (
        spark.table(f"{silver}.{name}")
        for name in ("entreprises", "dirigeants", "annonces_bodacc", "liens")
    )
    write(
        fiche_entreprise(companies, dirs, notices),
        f"{gold}.fiche_entreprise",
        ["siren"],
        properties,
    )
    write(
        reseau_entreprises(edges, companies),
        f"{gold}.reseau_entreprises",
        ["siren_a", "siren_b"],
        properties,
    )
    write(
        indicateurs_territoire(companies, notices),
        f"{gold}.indicateurs_territoire",
        ["mois", "departement"],
        properties,
    )
    print(json.dumps({"event": "etl_completed", "run": run_id, "sample": args.siren_prefix}))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", required=True, help="Racine des données, ex. gs://BUCKET")
    parser.add_argument("--catalog", default="industrial_intelligence")
    parser.add_argument("--silver", default="silver_spark", help="Namespace Iceberg silver")
    parser.add_argument("--gold", default="gold", help="Namespace Iceberg gold")
    parser.add_argument("--rne-stock", default="stock_RNE_formalites_NIVEAU1_20260304_1400")
    parser.add_argument(
        "--siren-prefix",
        default="",
        help="Échantillon : seulement les SIREN commençant par ce préfixe (ex. 30 ≈ 1 %%)",
    )
    args = parser.parse_args(argv)
    if args.siren_prefix and not args.siren_prefix.isdigit():
        parser.error("--siren-prefix must be digits")
    run(SparkSession.builder.appName("industrial-intelligence-etl").getOrCreate(), args)


if __name__ == "__main__":
    main()
