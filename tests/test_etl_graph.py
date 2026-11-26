import json
import logging
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from industrial_intelligence.etl import graph

T0 = datetime(2026, 3, 1)
T1 = datetime(2026, 9, 1)
RNE_DDL = (
    "siren STRING, source_updated_at TIMESTAMP, source_retrieved_at TIMESTAMP,"
    " source_kind STRING, company_sha256 STRING, type_personne STRING, denomination STRING,"
    " forme_juridique STRING, code_ape STRING, diffusion_commerciale BOOLEAN, deleted BOOLEAN,"
    " company_json STRING"
)


@pytest.fixture(scope="module")
def spark():
    # py4j logs its shutdown after pytest has closed the captured streams.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def person(nom, prenoms, naissance="1970-05"):
    return {"nom": nom, "prenoms": prenoms, "dateDeNaissance": naissance}


def morale(denomination, pouvoirs=(), capital=None):
    return {
        "formality": {
            "content": {
                "personneMorale": {
                    "identite": {
                        "entreprise": {"denomination": denomination, "dateImmat": "2001-02-03"},
                        "description": {"montantCapital": capital},
                    },
                    "composition": {"pouvoirs": list(pouvoirs)},
                }
            }
        }
    }


def individu(nom, prenoms, naissance="1970-05", role="30"):
    return {
        "roleEntreprise": role,
        "typeDePersonne": "INDIVIDU",
        "actif": True,
        "individu": {"descriptionPersonne": person(nom, prenoms, naissance)},
    }


def societe(siren, role="73"):
    return {
        "roleEntreprise": role,
        "typeDePersonne": "ENTREPRISE",
        "entreprise": {"siren": siren, "denomination": "HOLDING"},
    }


def name(doc):
    return doc["formality"]["content"]["personneMorale"]["identite"]["entreprise"]["denomination"]


def rne_rows(spark, *rows):
    # Bronze already carries the denomination extracted at ingestion, as in companies.py.
    data = [
        (siren, updated, T0, kind, sha, "M", name(doc), None, None, True, False, json.dumps(doc))
        for siren, updated, kind, sha, doc in rows
    ]
    return spark.createDataFrame(data, RNE_DDL)


def test_rne_current_keeps_latest_record_and_prefers_diff_on_ties(spark):
    rne = rne_rows(
        spark,
        ("000000001", T0, "stock", "a", morale("ANCIEN")),
        ("000000001", T1, "stock", "b", morale("STOCK")),
        ("000000001", T1, "diff", "c", morale("DIFF", capital=1000.5)),
        ("000000002", T0, "stock", "d", morale("SEULE")),
    )
    current = graph.rne_current(rne).orderBy("siren").collect()

    assert [r.siren for r in current] == ["000000001", "000000002"]
    first = current[0].doc.formality.content.personneMorale.identite
    assert first.entreprise.denomination == "DIFF"
    assert first.description.montantCapital == 1000.5


def test_dirigeants_and_person_matching_across_companies(spark):
    alice = individu("Martin", ["Alice", "Marie"])
    rne = rne_rows(
        spark,
        ("000000001", T0, "stock", "a", morale("A", [alice, societe("552 100 554")])),
        ("000000002", T0, "stock", "b", morale("B", [alice, individu("Martin", ["Alice"])])),
        ("000000003", T0, "stock", "c", morale("C", [individu("Sans", ["Date"], None)])),
    )
    dirs = graph.dirigeants(graph.rne_current(rne)).collect()

    by_company = {}
    for row in dirs:
        by_company.setdefault(row.siren, []).append(row)
    pm = next(r for r in by_company["000000001"] if r.type_personne == "ENTREPRISE")
    assert pm.siren_dirigeant == "552100554" and pm.personne_id is None
    ids = {r.siren: r.personne_id for r in dirs if r.prenoms == "Alice Marie"}
    assert ids["000000001"] == ids["000000002"] is not None
    # Different first names: another person; no birth date: no identity guessed.
    other = next(r for r in by_company["000000002"] if r.prenoms == "Alice")
    assert other.personne_id not in (None, ids["000000001"])
    assert by_company["000000003"][0].personne_id is None


BODACC_DDL = (
    "id STRING, dateparution DATE, numeroannonce BIGINT, familleavis STRING,"
    " familleavis_lib STRING, typeavis STRING, tribunal STRING, numerodepartement STRING,"
    " ville STRING, cp STRING, commercant STRING, registre STRING,"
    " listeprecedentproprietaire STRING, jugement STRING, depot STRING, url_complete STRING,"
    " _file STRING, _modified TIMESTAMP"
)


def notice(id_, famille, registre, *, sellers=None, jugement=None, file="f1", modified=T0):
    previous = (
        json.dumps(
            {"personne": [{"numeroImmatriculation": {"numeroIdentification": s}} for s in sellers]}
        )
        if sellers
        else None
    )
    return (
        id_,
        date(2026, 11, 17),
        1,
        famille,
        None,
        None,
        None,
        "75",
        None,
        None,
        None,
        registre,
        previous,
        json.dumps(jugement) if jugement else None,
        None,
        None,
        file,
        modified,
    )


def test_annonces_parse_registry_dedupe_and_sale_edges(spark):
    bodacc = spark.createDataFrame(
        [
            notice("A-1", "collective", "111111111,111 111 111", jugement={"nature": "old"}),
            notice(
                "A-1",
                "collective",
                "111111111,111 111 111",
                jugement={"nature": "Jugement d'ouverture"},
                file="f2",
                modified=T1,
            ),
            notice(
                "V-1", "vente", "222222222,222 222 222,333333333,12345", sellers=["333 333 333"]
            ),
        ],
        BODACC_DDL,
    )
    notices = graph.annonces(bodacc)
    rows = {r.id: r for r in notices.collect()}

    assert rows["A-1"].jugement_nature == "Jugement d'ouverture"
    assert rows["A-1"].sirens == ["111111111"]
    assert rows["V-1"].sirens == ["222222222", "333333333"]
    assert rows["V-1"].sirens_cedants == ["333333333"]

    empty_dirs = graph.dirigeants(graph.rne_current(rne_rows(spark)))
    succ = spark.createDataFrame(
        [
            ("11111111100011", "11111111100029", date(2020, 1, 1), False, True, T0),
            ("11111111100037", "44444444400015", date(2021, 1, 1), True, True, T0),
        ],
        "siretEtablissementPredecesseur STRING, siretEtablissementSuccesseur STRING,"
        " dateLienSuccession DATE, transfertSiege BOOLEAN, continuiteEconomique BOOLEAN,"
        " dateDernierTraitementLienSuccession TIMESTAMP",
    )
    doublons = spark.createDataFrame(
        [], "siren STRING, sirenDoublon STRING, dateDernierTraitementDoublon TIMESTAMP"
    )
    edges = graph.liens(empty_dirs, succ, doublons, notices).collect()

    sale = [e for e in edges if e.type_lien == "cession_fonds"]
    assert [(e.de_id, e.vers_id, e.reference, e.preuve) for e in sale] == [
        ("333333333", "222222222", "V-1", "fait")
    ]
    # Only the succession that crosses two companies becomes a company-level edge.
    between = [e for e in edges if e.type_lien == "succession_entre_entreprises"]
    assert [(e.de_id, e.vers_id) for e in between] == [("111111111", "444444444")]


def test_reseau_links_companies_sharing_a_representative_up_to_the_cap(spark, monkeypatch):
    monkeypatch.setattr(graph, "MAX_SHARED_MANDATES", 2)
    edges = spark.createDataFrame(
        [
            ("mandat_personne_physique", "personne", "p1", "entreprise", "000000001"),
            ("mandat_personne_physique", "personne", "p1", "entreprise", "000000002"),
            # A professional representative of 3 companies (> cap): no pairs from it.
            ("mandat_personne_morale", "entreprise", "999999999", "entreprise", "000000001"),
            ("mandat_personne_morale", "entreprise", "999999999", "entreprise", "000000002"),
            ("mandat_personne_morale", "entreprise", "999999999", "entreprise", "000000003"),
        ],
        "type_lien STRING, de_type STRING, de_id STRING, vers_type STRING, vers_id STRING",
    )
    edges = edges.selectExpr(
        "*",
        "CAST(NULL AS STRING) date_lien",
        "CAST(NULL AS STRING) role_code",
        "'fait' preuve",
        "'rne' source",
        "CAST(NULL AS STRING) reference",
    )
    companies = spark.createDataFrame(
        [("000000001", "A"), ("000000002", "B"), ("000000003", "C"), ("999999999", "PRO")],
        "siren STRING, denomination STRING",
    )
    pairs = graph.reseau_entreprises(edges, companies)
    shared = [
        (r.siren_a, r.siren_b, r.preuve)
        for r in pairs.collect()
        if r.type_lien == "dirigeant_commun"
    ]
    assert shared == [("000000001", "000000002", "inference")]
    assert not [r for r in pairs.collect() if r.type_lien == "personne_morale_commune"]


UL_DDL = (
    "siren STRING, statutDiffusionUniteLegale STRING, dateCreationUniteLegale DATE,"
    " sigleUniteLegale STRING, prenom1UniteLegale STRING, prenomUsuelUniteLegale STRING,"
    " trancheEffectifsUniteLegale STRING, anneeEffectifsUniteLegale BIGINT,"
    " categorieEntreprise STRING, dateDebut DATE, etatAdministratifUniteLegale STRING,"
    " nomUniteLegale STRING, nomUsageUniteLegale STRING, denominationUniteLegale STRING,"
    " categorieJuridiqueUniteLegale BIGINT, activitePrincipaleUniteLegale STRING,"
    " nicSiegeUniteLegale STRING, economieSocialeSolidaireUniteLegale STRING,"
    " societeMissionUniteLegale STRING, caractereEmployeurUniteLegale STRING,"
    " activitePrincipaleNAF25UniteLegale STRING"
)
ETAB_DDL = (
    "siret STRING, siren STRING, nic STRING, etablissementSiege BOOLEAN,"
    " etatAdministratifEtablissement STRING, dateCreationEtablissement DATE, dateDebut DATE,"
    " activitePrincipaleEtablissement STRING, trancheEffectifsEtablissement STRING,"
    " caractereEmployeurEtablissement STRING, enseigne1Etablissement STRING,"
    " denominationUsuelleEtablissement STRING, numeroVoieEtablissement STRING,"
    " indiceRepetitionEtablissement STRING, typeVoieEtablissement STRING,"
    " libelleVoieEtablissement STRING, codePostalEtablissement STRING,"
    " libelleCommuneEtablissement STRING, codeCommuneEtablissement STRING,"
    " statutDiffusionEtablissement STRING"
)


def ul(siren, denomination, nic="00011", prenom=None, nom=None):
    values = dict.fromkeys(name.split()[0] for name in UL_DDL.split(","))
    values.update(
        siren=siren,
        denominationUniteLegale=denomination,
        nicSiegeUniteLegale=nic,
        prenom1UniteLegale=prenom,
        nomUniteLegale=nom,
        categorieJuridiqueUniteLegale=5710,
    )
    return tuple(values.values())


def etab(siret, siege, etat, commune):
    values = dict.fromkeys(name.split()[0] for name in ETAB_DDL.split(","))
    values.update(
        siret=siret,
        siren=siret[:9],
        etablissementSiege=siege,
        etatAdministratifEtablissement=etat,
        codeCommuneEtablissement=commune,
        libelleVoieEtablissement="  RUE DE LA PAIX ",
    )
    return tuple(values.values())


def test_entreprises_joins_sirene_rne_and_head_office(spark):
    units = spark.createDataFrame(
        [ul("000000001", " ACME "), ul("000000002", "[ND]", prenom="Jean", nom="Durand")],
        UL_DDL,
    )
    etabs = graph.etablissements(
        spark.createDataFrame(
            [
                etab("00000000100011", True, "A", "97411"),
                etab("00000000100029", False, "F", "75056"),
            ],
            ETAB_DDL,
        )
    )
    rne = graph.rne_current(rne_rows(spark, ("000000009", T0, "stock", "a", morale("RNE SEUL"))))
    rows = {r.siren: r for r in graph.entreprises(units, rne, etabs).collect()}

    assert rows["000000001"].denomination == "ACME"
    assert rows["000000001"].siege_departement == "974"
    assert rows["000000001"].siege_adresse == "RUE DE LA PAIX"
    assert (rows["000000001"].nb_etablissements, rows["000000001"].nb_etablissements_actifs) == (
        2,
        1,
    )
    assert rows["000000001"].categorie_juridique == "5710"
    # '[ND]' is "not disclosed", not a name: the individual's name is built from its parts.
    assert rows["000000002"].denomination == "Jean Durand"
    only_rne = rows["000000009"]
    assert (only_rne.dans_sirene, only_rne.dans_rne) == (False, True)
    assert only_rne.denomination == "RNE SEUL" and only_rne.date_immatriculation == "2001-02-03"


def test_require_identifiers_stops_on_short_siren(spark):
    good = spark.createDataFrame([("000000001",)], "siren STRING")
    graph.require_identifiers(good, "t", {"siren": 9})
    with pytest.raises(ValueError, match="invalid identifier"):
        graph.require_identifiers(good.union(spark.createDataFrame([("1",)])), "t", {"siren": 9})


def test_sample_keeps_bodacc_notices_whose_registered_siren_starts_with_prefix(spark):
    one = spark.createDataFrame([("300000001",)], "siren STRING")
    frames = {
        "unite_legale": one,
        "etablissement": one,
        "doublons": one,
        "rne": one,
        "etablissement_liens_succession": spark.createDataFrame(
            [("30000000100011",)], "siretEtablissementPredecesseur STRING"
        ),
        "bodacc": spark.createDataFrame(
            [
                ("a", "300000001,300 000 001"),
                ("b", "552 100 554,123300456"),
                ("c", "552100554,300 1"),
            ],
            "id STRING, registre STRING",
        ),
    }
    kept = graph.sample(frames, "30")["bodacc"].orderBy("id").collect()
    # "123300456" contains 30 but does not start with it.
    assert [r.id for r in kept] == ["a", "c"]


def test_indicateurs_do_not_count_the_1900_placeholder_as_a_creation(spark):
    companies = spark.createDataFrame(
        [
            ("000000001", date(1900, 1, 1), "75", "56.10A"),
            ("000000002", date(2026, 5, 3), "75", "56.10A"),
        ],
        "siren STRING, date_creation DATE, siege_departement STRING, activite_principale STRING",
    )
    notices = spark.createDataFrame(
        [], "dateparution DATE, departement STRING, famille STRING, sirens ARRAY<STRING>"
    )
    rows = graph.indicateurs_territoire(companies, notices).collect()
    assert [(r.mois, r.creations_sirene) for r in rows] == [(date(2026, 5, 1), 1)]
