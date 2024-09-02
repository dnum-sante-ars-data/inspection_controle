import pandas as pd
import sqlite3 
import json
from Modules.init_db.init_db import connDb
from Modules.utils.utils import read_settings

# Connexion à la base et création des tables

def drop_existing_views(cursor, table):
    for table in table:
        try:
            cursor.execute(f"DROP VIEW IF EXISTS {table}")
            print(f"La table {table} a été supprimée.")
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                # Si la vue n'existe pas, ignorer cette erreur
                print(f"Aucune table de ce type : {table}")
            else:
                # Si c'est une autre erreur, essayer de la supprimer comme une table
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    print(f"La table {table} a été supprimée.")
                except sqlite3.OperationalError as e:
                    print(f"Échec de la suppression de {table} : {e}")
                    
                    
def inittable(conn):
    dbname = read_settings("settings/settings.json", "db", "name")
    conn = connDb(dbname)
    cursor = conn.cursor()
    
 # Listes des vues à supprimer
    table = [
        "lien_communes", "missions_prev", "t_finess_500", " missions_real","mission_real_complet", "compte_ehpad", "compte_ehpad_controles", "compte_missions","reference", "communes","departements",
        "regions", "missions_prev_complet", "cross_miss_sui", 
        "contrainte", "concat_theme", "group_mottif_sante_env", "groupe_diamant",
        "ods_ic_complet", "cross_miss_sui_suites", "ehpad_control",
        "missions_real_dwh", "missions_clot", 
        "missions_clo_ss_s", "saisines_parq", 
        "injonctions", "prescriptions", 
        "injonc_prescr", "missions_real_tdb", "missions_clo_ss_s_tdb",
        "injonctions_tdb", "prescriptions_tdb", "DWH_SUITES"]
  # Supprimer les vues existantes
    drop_existing_views(cursor, table)
    
    
    #Création des tables intermediares pour la selection de mes tables finales         
    lien_communes= f"""
     Create Table lien_communes AS
     SELECT 
     com,ncc,dep,reg
     FROM ref_insee_communes
     WHERE reg != '' """
    cursor.execute(lien_communes)
    conn.commit()
    print("lien_communes a été ajouté")
    
    
    missions_prev = """
        CREATE TABLE missions_prev AS
        SELECT 
            *,
            CASE 
                WHEN LENGTH("Code FINESS") = 8 THEN '0' || "Code FINESS"
                ELSE "Code FINESS"
            END AS CD_FINESS
        FROM sa_missions_prev
        WHERE "Secteur d'intervention" = 'Médico-social'
        AND "Type de cible" = 'Etablissements et Services pour Personnes Agées'
        AND "Code thème IGAS" IN (
            'MS634D13',
            'MS634N1',
            'MS634E1',
            'MS634D12',
            'MS634R1',
            'MS634D11',
            'MS634D15',
            'MS634C10'
        )
        AND "Type de mission" NOT IN (
            'Audit',
            'Audit franco-wallon',
            'Evaluation',
            'Visites de conformité',
            'Enquête administrative'
        )
        -- Conversion de la date texte en format YYYYMMDD pour la comparaison
        AND CAST(
            SUBSTR('Date provisoire ""Visite"', 1, 4) || 
            SUBSTR('Date provisoire ""Visite"', 6, 2) || 
            SUBSTR('Date provisoire ""Visite"', 9, 4)
            AS INTEGER
        ) <20240630
        AND CAST(
            SUBSTR('Date réelle "Visite"', 1, 4) || 
            SUBSTR('Date réelle "Visite"', 6, 2) || 
            SUBSTR('Date réelle "Visite"', 9, 4)
            AS INTEGER
        ) < 20240630;"""

    # Execute the corrected SQL
    cursor.execute(missions_prev)
    conn.commit()
    print("missions_prev a été ajouté")

    
    t_finess_500= f"""
        Create table t_finess_500 AS
        SELECT x.*,x.rowid FROM t_finess x
        where categ_code = "500"
        and etat = "ACTUEL" """
    cursor.execute(t_finess_500)
    conn.commit()
    print("t_finess_500 a été ajouté")
    
    missions_real=f"""
    Create table missions_real AS
            SELECT 
            *,
            CASE 
                WHEN LENGTH("Code FINESS") = 8 THEN '0' || "Code FINESS"
                ELSE "Code FINESS"
            END AS CD_FINESS
        FROM sa_missions_real
        WHERE "Secteur d'intervention" = 'Médico-social'
        AND "Type de cible" = 'Etablissements et Services pour Personnes Agées'
        AND "Code thème IGAS" IN (
            'MS634D13',
            'MS634N1',
            'MS634E1',
            'MS634D12',
            'MS634R1',
            'MS634D11',
            'MS634D15',
            'MS634C10'
        )
        AND "Type de mission" NOT IN (
            'Audit',
            'Audit franco-wallon',
            'Evaluation',
            'Visites de conformité',
            'Enquête administrative'
        )
        AND "Statut de la mission" IN (
            'Clôturé',
            'Maintenu'
        )
        -- Filtrage sur la date pour inclure les missions entre le 01/01 de N-3 et le dernier jour du dernier trimestre de N
        AND "Date réelle Visite" BETWEEN '2019-01-01' AND '2024-06-30';"""
    cursor.execute(missions_real)
    conn.commit()
    print("missions_real a été ajouté")
    
    
    mission_real_complet= f"""
        Create Table mission_real_complet AS
        SELECT
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        missions_real.CD_FINESS AS finess_cd,
        missions_real.Cible,
        missions_real."Identifiant de la mission",
        t_finess.statut_jur_niv2_code AS statut_juridique_cd,
        IIF(t_finess.statut_jur_niv2_lib = '', "NC", t_finess.statut_jur_niv2_lib) AS statut_juridique_lb,
        missions_real."Type de mission" AS type_de_mission,
        CASE 
            WHEN missions_real."Type de mission" = "contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Contrôle sur pièces EHPAD" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "EHPAD Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Ctrl_sur_Pièces" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Inspection" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Inspection Technique" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "inspection" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Evaluation" THEN "Evaluation"
            WHEN missions_real."Type de mission" = "Contrôle" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Enquête administrative" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Visites de conformité" THEN "Visites de conformité"
            WHEN missions_real."Type de mission" = "Contrôle sur place / Visite de vérification" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Inspection_SE" THEN "Inspection santé-environnement"
            WHEN missions_real."Type de mission" = "Inspection_prgm21" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Inspection_prgm23" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Inspection_prgm24" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "contrôle" THEN "Contrôle sur place"
            WHEN missions_real."Type de mission" = "Contrôle sur pièces (Avec contradictoire)" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Controle sur pièces contradictoire" THEN "Contrôle sur pièces"
            WHEN missions_real."Type de mission" = "Suites d'inspection" THEN "Contrôle sur place"
            ELSE "NC"
        END AS CTRL_PL_PI,
        missions_real."Statut de la mission",
        missions_real."Date réelle Visite",
        sa_cibles.[Groupe de cibles] AS groupe_siicea,
        IIF(missions_real."Type de planification" = 'Inopiné', 'Programmé', missions_real."Type de planification") AS "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        CASE 
            WHEN ("Mission conjointe avec 1" LIKE "%Conseil départemental%" OR "Mission conjointe avec 1" LIKE "%Département%" OR "Mission conjointe avec 2" LIKE "%Conseil départemental%") THEN "ARS / CD"
            WHEN "Mission conjointe avec 1" = '' OR "Mission conjointe avec 1" = 'Non' THEN "Non conjointe"
            ELSE "ARS + autre administration"
        END AS mission_conjointe,
        IIF("Modalité de la mission"='', 'NC', "Modalité de la mission") AS "Modalité de la mission"
        FROM missions_real
        LEFT JOIN t_finess ON missions_real.CD_FINESS = t_finess.finess
        LEFT JOIN lien_communes ON t_finess.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd
        LEFT JOIN sa_cibles ON missions_real.CD_FINESS = sa_cibles.FINESS """
    cursor.execute(mission_real_complet)
    conn.commit()
    print("mission_real_complet a été ajouté")
    
    
    compte_ehpad= f"""
        Create table compte_ehpad AS
        SELECT 
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) || IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) || IIF(t_finess_500.com_code IS NULL, "NC", t_finess_500.com_code) as id_ref,
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess_500.com_code IS NULL, "NC", t_finess_500.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        finess,
        rs
        FROM t_finess_500
        LEFT JOIN lien_communes ON t_finess_500.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd"""
    cursor.execute(compte_ehpad)
    conn.commit()
    print("compte_ehpad a été ajouté")   
    
    compte_ehpad_controles= f"""
        Create table compte_ehpad_controles AS 
        SELECT 
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) || IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) || IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as id_ref,
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        CD_FINESS
        FROM missions_real  
        LEFT JOIN t_finess ON missions_real.CD_FINESS = t_finess.finess
        LEFT JOIN lien_communes ON t_finess.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd """
    cursor.execute(compte_ehpad_controles)
    conn.commit()
    print("compte_ehpad_controles a été ajouté")     
    
    compte_missions = f"""
        Create table compte_missions AS 
        SELECT 
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) || IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) || IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as id_ref,
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        "Identifiant de la mission"
        FROM missions_real
        LEFT JOIN t_finess ON missions_real.CD_FINESS = t_finess.finess
        LEFT JOIN lien_communes ON t_finess.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd"""
    cursor.execute(compte_missions)
    conn.commit()
    print("compte_missions a été ajouté")
         
    reference = f"""
        Create table reference AS 
        SELECT 
        id_ref,
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb
        FROM compte_ehpad
        UNION 
        SELECT 
        id_ref,
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb
        FROM compte_ehpad_controles
        UNION 
        SELECT 
        id_ref,
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb
        FROM compte_missions """
    cursor.execute(reference)
    conn.commit()
    print("reference a été ajouté")       
    
    communes = f"""
        Create table communes AS 
        SELECT 
        reference.id_ref,
        reference.reg_cd,
        reference.reg_lb,
        reference.dep_cd,
        reference.dep_lb,
        reference.com_cd,
        reference.com_lb,
        COUNT(DISTINCT finess) AS NB_EHPAD,
        COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSION,
        COUNT(DISTINCT CD_FINESS) AS NB_ETAB_CONTROLE,
        (CAST(COUNT(DISTINCT CD_FINESS) AS FLOAT)/CAST(COUNT(DISTINCT finess) AS FLOAT))*100 AS NB_ETAB_CONTROLE_NB_EHPAD
        FROM reference
        LEFT JOIN compte_ehpad ON reference.id_ref = compte_ehpad.id_ref
        LEFT JOIN compte_ehpad_controles ON reference.id_ref = compte_ehpad_controles.id_ref
        LEFT JOIN compte_missions ON reference.id_ref = compte_missions.id_ref
        GROUP BY 
        reference.id_ref,
        reference.reg_cd,
        reference.reg_lb,
        reference.dep_cd,
        reference.dep_lb,
        reference.com_cd,
        reference.com_lb """
    cursor.execute(communes)
    conn.commit()
    print("communes a été ajouté") 
    
    departements= f"""
        Create table departements AS 
        SELECT 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        SUM(NB_EHPAD) AS NB_EHPAD,
        SUM(NB_MISSION) AS NB_MISSION,
        SUM(NB_ETAB_CONTROLE) AS NB_ETAB_CONTROLE,
        (CAST(SUM(NB_ETAB_CONTROLE) AS FLOAT)/CAST(SUM(NB_EHPAD) AS FLOAT))*100 AS NB_ETAB_CONTROLE_NB_EHPAD
        FROM communes
        GROUP BY 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb """
    cursor.execute(departements)
    conn.commit()
    print("departements a été ajouté") 
    
    regions= f"""       
        Create table regions AS
        SELECT 
        reg_cd,
        reg_lb,
        SUM(NB_EHPAD) AS NB_EHPAD,
        SUM(NB_MISSION) AS NB_MISSION,
        SUM(NB_ETAB_CONTROLE) AS NB_ETAB_CONTROLE,
        (CAST(SUM(NB_ETAB_CONTROLE) AS FLOAT)/CAST(SUM(NB_EHPAD) AS FLOAT))*100 AS NB_ETAB_CONTROLE_NB_EHPAD
        FROM communes
        GROUP BY 
        reg_cd,
        reg_lb """
    cursor.execute(regions)
    conn.commit()
    print("regions a été ajouté")   

    missions_prev_complet= f"""
        Create table missions_prev_complet AS 
        SELECT
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        missions_prev.CD_FINESS AS finess_cd,
        missions_prev.Cible,
        missions_prev."Identifiant de la mission",
        t_finess.statut_jur_niv2_code AS statut_juridique_cd,
        IIF(t_finess.statut_jur_niv2_lib = '', "NC", t_finess.statut_jur_niv2_lib) AS statut_juridique_lb,
        missions_prev."Type de mission" AS type_de_mission,
        CASE 
            WHEN missions_prev."Type de mission" = "contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Contrôle sur pièces EHPAD" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "EHPAD Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Ctrl_sur_Pièces" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Inspection" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Inspection Technique" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "inspection" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Evaluation" THEN "Evaluation"
            WHEN missions_prev."Type de mission" = "Contrôle" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Enquête administrative" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Visites de conformité" THEN "Visites de conformité"
            WHEN missions_prev."Type de mission" = "Contrôle sur place / Visite de vérification" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Inspection_SE" THEN "Inspection santé-environnement"
            WHEN missions_prev."Type de mission" = "Inspection_prgm21" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Inspection_prgm23" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Inspection_prgm24" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "contrôle" THEN "Contrôle sur place"
            WHEN missions_prev."Type de mission" = "Contrôle sur pièces (Avec contradictoire)" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Controle sur pièces contradictoire" THEN "Contrôle sur pièces"
            WHEN missions_prev."Type de mission" = "Suites d'inspection" THEN "Contrôle sur place"
            ELSE "NC"
        END AS CTRL_PL_PI,
        missions_prev."Statut de la mission",
        missions_prev.'Date provisoire "Visite"',
        sa_cibles.[Groupe de cibles] AS groupe_siicea,
        IIF(missions_prev."Type de planification" = 'Inopiné', 'Programmé', missions_prev."Type de planification") AS "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        CASE 
            WHEN ("Mission conjointe avec 1" LIKE "%Conseil départemental%" OR "Mission conjointe avec 1" LIKE "%Département%" OR "Mission conjointe avec 2" LIKE "%Conseil départemental%") THEN "ARS / CD"
            WHEN "Mission conjointe avec 1" = '' OR "Mission conjointe avec 1" = 'Non' THEN "Non conjointe"
            ELSE "ARS + autre administration"
        END AS mission_conjointe,
        IIF("Modalité de la mission"='', 'NC', "Modalité de la mission") AS "Modalité de la mission"
        FROM missions_prev
        LEFT JOIN t_finess ON missions_prev.CD_FINESS = t_finess.finess
        LEFT JOIN lien_communes ON t_finess.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd
        LEFT JOIN sa_cibles ON missions_prev.CD_FINESS = sa_cibles.FINESS """
            
    cursor.execute(missions_prev_complet)
    conn.commit()
    print("missions_prev_complet a été ajouté")  
    
    cross_miss_sui= f"""
        Create table cross_miss_sui AS 
        SELECT 
            "reg_cd",
            "reg_lb",
            "dep_cd",
            "dep_lb",
            "com_cd",
            "com_lb",
            "finess_cd",
            "Cible",
            mission_real_complet."Identifiant de la mission",
            "statut_juridique_cd",
            "statut_juridique_lb",
            '' AS groupe,
            "type_de_mission",
            "CTRL_PL_PI",
            '' AS group_hsa_sa,
            '' AS filtre,
            "Statut de la mission",
            "Date réelle Visite", -- Corrigé pour éviter les doubles guillemets imbriqués
            '' AS GROUPE_2,
            "groupe_siicea",
            "Type de planification",
            "Mission conjointe avec 1",
            "Mission conjointe avec 2",
            "mission_conjointe",
            "Modalité de la mission",
            decisions."Type de décision", -- Assurez-vous que cela correspond bien à la table decisions
            decisions."Complément", -- Assurez-vous que cela correspond bien à la table decisions
            decisions."Thème Décision", -- Assurez-vous que cela correspond bien à la table decisions
            decisions."Sous-thème Décision", -- Assurez-vous que cela correspond bien à la table decisions
            COALESCE(decisions.SANCTION, 'sans_contrainte') AS SANCTION, -- Utilisation correcte de COALESCE
            SUM(decisions.Nombre) AS NB_SUITE -- Agrégation correcte avec SUM
        FROM mission_real_complet
        LEFT JOIN (
            SELECT 
                "Identifiant de la mission",
                "Type de décision",
                "Complément",
                "Thème Décision",
                "Sous-thème Décision",
                IIF("Type de décision" IN ('Injonction', 'Prescription', 'Saisine'), 'contrainte', 'sans_contrainte') AS SANCTION,
                Nombre
            FROM "sa_decisions"
        ) decisions 
            ON mission_real_complet."Identifiant de la mission" = decisions."Identifiant de la mission"
        GROUP BY 
            "reg_cd",
            "reg_lb",
            "dep_cd",
            "dep_lb",
            "com_cd",
            "com_lb",
            "finess_cd",
            "Cible",
            mission_real_complet."Identifiant de la mission",
            "statut_juridique_cd",
            "statut_juridique_lb",
            "type_de_mission",
            "CTRL_PL_PI",
            "Statut de la mission",
            "Date réelle Visite", 
            "groupe_siicea",
            "Type de planification",
            "Mission conjointe avec 1",
            "Mission conjointe avec 2",
            "mission_conjointe",
            "Modalité de la mission",
            decisions."Type de décision", 
            decisions."Complément",
            decisions."Thème Décision",
            decisions."Sous-thème Décision",
            COALESCE(decisions.SANCTION, 'sans_contrainte');
        """
    cursor.execute(cross_miss_sui)
    conn.commit()
    print("cross_miss_sui a été ajouté") 
    
    contrainte= f"""
        Create table contrainte AS 
        SELECT 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        "Identifiant de la mission",
        statut_juridique_cd,
        statut_juridique_lb,
        groupe,
        type_de_mission,
        CTRL_PL_PI,
        group_hsa_sa,
        filtre,
        "Statut de la mission",
        "Date réelle Visite",
        GROUPE_2,
        groupe_siicea,
        "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        mission_conjointe,
        "Modalité de la mission",
        "avec sanction" AS AVEC_SANCTION
        FROM cross_miss_sui
        WHERE SANCTION = "contrainte"
        GROUP BY
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        "Identifiant de la mission",
        statut_juridique_cd,
        statut_juridique_lb,
        groupe,
        type_de_mission,
        CTRL_PL_PI,
        group_hsa_sa,
        filtre,
        "Statut de la mission",
        "Date réelle Visite",
        GROUPE_2,
        groupe_siicea,
        "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        mission_conjointe,
        "Modalité de la mission" """
    cursor.execute(contrainte)
    conn.commit()
    print("contrainte a été ajouté") 
    
    concat_theme=f"""
        Create table concat_theme AS 
        SELECT 
        "Identifiant de la mission",
        COUNT(*),
        GROUP_CONCAT("Code thème IGAS") AS concat,
        GROUP_CONCAT("Thème IGAS") AS theme
        FROM ODS_IC
        GROUP BY
        "Identifiant de la mission"  """
    cursor.execute(concat_theme)
    conn.commit()
    print("concat_theme a été ajouté")  
    
    group_mottif_sante_env= f"""
        Create table group_mottif_sante_env AS 
        SELECT 
        concat_theme.*,
        IIF(
        concat LIKE '%MS634C10%'
        OR concat LIKE '%MS634D11%'
        OR concat LIKE '%MS634D12%'
        OR concat LIKE '%MS634D13%'
        OR concat LIKE '%MS634D14%'
        OR concat LIKE '%MS634E1%'
        OR concat LIKE '%MS634N1%'
        OR concat LIKE '%MS634R1%'
        OR concat LIKE '%MS634S1%'
        , 'motif_hsa', 'motif_sa') AS group_hsa_sa
        FROM concat_theme  """
    cursor.execute(group_mottif_sante_env)
    conn.commit()
    print("group_mottif_sante_env a été ajouté")
    
    
    groupe_diamant=f"""
        Create table groupe_diamant AS 
        SELECT
        CASE 
        WHEN LENGTH(FINESS)<9 THEN "0" || FINESS
        ELSE FINESS
        END AS FINESS,
        GROUPE 
        FROM diamant_orpea
        UNION
        SELECT 
        CASE 
        WHEN LENGTH(nofinesset)<9 THEN "0" || nofinesset
        ELSE nofinesset
        END AS FINESS,
        "LNA/BRIDGE" AS GROUPE
        FROM diamant_groupe_lna_bridge
        UNION
        SELECT 
        IIF(LENGTH(finess)=8, '0' || finess, finess) AS FINESS,
        'KORIAN' AS GROUPE
        FROM korian_diamant
        UNION
        -- 2 FINESS KORIAN manquant dans DIAMANT après comparaison site officiel Korian
        SELECT 
        IIF(LENGTH(finess)=8, '0' || finess, finess) AS FINESS,
        'KORIAN' AS GROUPE
        FROM t_finess
        WHERE finess IN (
        '470014515',
        '330802968'
        )"""
    cursor.execute(groupe_diamant)
    conn.commit()
    print("groupe_diamant a été ajouté")
    
    ods_ic_complet=f"""
        Create table ods_ic_complet AS 
        SELECT
        IIF(ref_insee_region.reg_cd IS NULL, "NC", ref_insee_region.reg_cd) as reg_cd,
        IIF(ref_insee_region.libelle IS NULL, "NC", ref_insee_region.libelle) as reg_lb,
        IIF(ref_insee_departement.DEP IS NULL, "NC", ref_insee_departement.DEP) as dep_cd,
        IIF(ref_insee_departement.LIBELLE IS NULL, "NC", ref_insee_departement.LIBELLE) as dep_lb,
        IIF(t_finess.com_code IS NULL, "NC", t_finess.com_code) as com_cd,
        IIF(lien_communes.ncc IS NULL, "NC", lien_communes.ncc) as com_lb,
        ODS_IC.CD_FINESS AS finess_cd,
        ODS_IC.Cible,
        ODS_IC."Identifiant de la mission",
        ODS_IC.statut_jur_niv2_code AS statut_juridique_cd,
        IIF(ODS_IC.statut_jur_niv2_lib = '', "NC", ODS_IC.statut_jur_niv2_lib) AS statut_juridique_lb,
        ODS_IC.RAISON_SOCIALE_SIREN AS groupe,
        ODS_IC."Type de mission" AS type_de_mission,
        CASE 
            WHEN ODS_IC."Type de mission" = "contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Contrôle sur pièces EHPAD" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "EHPAD Contrôle sur pièces" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Ctrl_sur_Pièces" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Inspection" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Inspection Technique" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "inspection" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Evaluation" THEN "Evaluation"
            WHEN ODS_IC."Type de mission" = "Contrôle" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Enquête administrative" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Visites de conformité" THEN "Visites de conformité"
            WHEN ODS_IC."Type de mission" = "Contrôle sur place / Visite de vérification" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Inspection_SE" THEN "Inspection santé-environnement"
            WHEN ODS_IC."Type de mission" = "Inspection_prgm21" THEN "Contrôle sur place"
                WHEN ODS_IC."Type de mission" = "Inspection_prgm23" THEN "Contrôle sur place"
                WHEN ODS_IC."Type de mission" = "Inspection_prgm24" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "contrôle" THEN "Contrôle sur place"
            WHEN ODS_IC."Type de mission" = "Contrôle sur pièces (Avec contradictoire)" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Controle sur pièces contradictoire" THEN "Contrôle sur pièces"
            WHEN ODS_IC."Type de mission" = "Suites d'inspection" THEN "Contrôle sur place"
            ELSE "NC"
        END AS CTRL_PL_PI,
        group_mottif_sante_env.group_hsa_sa,
        IIF(group_mottif_sante_env.group_hsa_sa = 'motif_hsa' AND ODS_IC."Type de mission" != 'Evaluation' AND ODS_IC."Type de mission" != 'Visites de conformité',"Hors santé-environnement","Santé-environnement") AS filtre,
        ODS_IC."Statut de la mission",
        ODS_IC."Date réelle ""Visite"",
        groupe_diamant.GROUPE AS GROUPE_2,
        sa_cibles.[Groupe de cibles] AS groupe_siicea,
        IIF(ODS_IC."Type de planification" = 'Inopiné', 'Programmé', ODS_IC."Type de planification") AS "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        CASE 
            WHEN ("Mission conjointe avec 1" LIKE "%Conseil départemental%" OR "Mission conjointe avec 1" LIKE "%Département%" OR "Mission conjointe avec 2" LIKE "%Conseil départemental%") THEN "ARS / CD"
            WHEN "Mission conjointe avec 1" = '' OR "Mission conjointe avec 1" = 'Non' THEN "Non conjointe"
            ELSE "ARS + autre administration"
        END AS mission_conjointe,
        IIF("Modalité de la mission"='', 'NC', "Modalité de la mission") AS "Modalité de la mission"
        FROM ODS_IC 
        LEFT JOIN t_finess ON ODS_IC.CD_FINESS = t_finess.finess
        LEFT JOIN lien_communes ON t_finess.com_code = lien_communes.com 
        LEFT JOIN ref_insee_departement ON lien_communes.dep = ref_insee_departement.DEP 
        LEFT JOIN ref_insee_region ON ref_insee_departement.REG = ref_insee_region.reg_cd
        LEFT JOIN group_mottif_sante_env ON ODS_IC."Identifiant de la mission" = group_mottif_sante_env."Identifiant de la mission"
        LEFT JOIN groupe_diamant ON ODS_IC.CD_FINESS = groupe_diamant.FINESS
        LEFT JOIN sa_cibles ON ODS_IC.CD_FINESS = sa_cibles.FINESS 
        WHERE "Type de mission" NOT IN (
        'Evaluation',
        'Visites de conformité', 
        'Audit franco-wallon')"""
    cursor.execute(ods_ic_complet)
    conn.commit()
    print("ods_ic_complet a été ajouté")
    
    cross_miss_sui_suites=f"""
        Create table cross_miss_sui_suites AS 
        SELECT 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        ods_ic_complet."Identifiant de la mission",
        statut_juridique_cd,
        IIF(statut_juridique_lb IS NULL, "NC", statut_juridique_lb) AS statut_juridique_lb,
        groupe,
        type_de_mission,
        CTRL_PL_PI,
        group_hsa_sa,
        filtre,
        "Statut de la mission",
        "Date réelle ""Visite"",
        GROUPE_2,
        groupe_siicea,
        "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        mission_conjointe,
        "Modalité de la mission",
        "Type de décision",
        Complément,
        "Thème Décision",
        "Sous-thème Décision",
        "Statut de décision",
        COALESCE(SANCTION, 'sans_contrainte') AS SANCTION,
        SUM(Nombre) AS NB_SUITE
        FROM ods_ic_complet
        LEFT JOIN 
            (SELECT 
            "Identifiant de la mission",
            "Type de décision",
            Complément,
            "Thème Décision",
            "Sous-thème Décision",
            "Statut de décision",
            IIF("Type de décision" IN (
                'Injonction',
                'Prescription',
                'Saisine'
                ), 'contrainte', 'sans_contrainte') AS SANCTION,
            Nombre
            FROM "sa_decisions"
            ) decisions ON ods_ic_complet."Identifiant de la mission"=decisions."Identifiant de la mission" 
        GROUP BY 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        ods_ic_complet."Identifiant de la mission",
        statut_juridique_cd,
        statut_juridique_lb,
        groupe,
        type_de_mission,
        CTRL_PL_PI,
        group_hsa_sa,
        filtre,
        "Statut de la mission",
        "Date réelle ""Visite"",
        GROUPE_2,
        groupe_siicea,
        "Type de planification",
        "Mission conjointe avec 1",
        "Mission conjointe avec 2",
        mission_conjointe,
        "Modalité de la mission",
        "Type de décision",
        Complément,
        "Thème Décision",
        "Sous-thème Décision",
        "Statut de décision",
        COALESCE(SANCTION, 'sans_contrainte')"""
    cursor.execute(cross_miss_sui_suites)
    conn.commit()
    print("cross_miss_sui_suites a été ajouté")
    
    
    ehpad_control=f"""
        Create table ehpad_control AS 
        SELECT
        reg_lb ,
        NB_ETAB_CONTROLE ,
        NB_ETAB_CONTROLE_NB_EHPAD 
        FROM DWH_MISSIONS_AGG_region
        ORDER BY reg_lb ASC """
    cursor.execute(ehpad_control)
    conn.commit()
    print("ehpad_control a été ajouté")
    
    missions_real_dwh=f"""
        Create table missions_real AS 
        SELECT 
        reg_lb ,
        SUM(NB_MISSION) AS NB_MISSIONS
        FROM DWH_MISSIONS
        GROUP BY reg_lb
        ORDER BY reg_lb ASC """
    cursor.execute(missions_real_dwh)
    conn.commit()
    print("missions_real a été ajouté")
    
    
    missions_clot=f"""
        Create table missions_clot AS 
        SELECT 
        reg_lb ,
        COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSIONS_CLOTUREES
        FROM DWH_MISSIONS_SANCTION
        --WHERE filtre = "Hors santé-environnement"
        GROUP BY reg_lb
        ORDER BY reg_lb ASC"""
    cursor.execute(missions_clot)
    conn.commit()
    print("missions_clot a été ajouté")

     
    missions_clo_ss_s=f"""
        Create table missions_clo_ss_s AS 
        SELECT 
        reg_lb ,
        COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSIONS_CLOTUREES_SANS_S
        FROM DWH_MISSIONS_SANCTION
        --WHERE filtre = "Hors santé-environnement" 
        WHERE SANCTION = "sans sanction"
        GROUP BY reg_lb
        ORDER BY reg_lb ASC"""
    cursor.execute(missions_clo_ss_s)
    conn.commit()
    print("missions_clo_ss_s a été ajouté")
    
    saisines_parq=f"""
        Create table saisines_parq AS 
        SELECT 
        reg_lb ,
        COUNT(DISTINCT "Identifiant de la mission") AS NB_SAISINES_PARQUET
        FROM DWH_SUITES
        WHERE Complément = "Saisine parquet"
        GROUP BY reg_lb
        ORDER BY reg_lb ASC"""
    cursor.execute(saisines_parq)
    conn.commit()
    print("saisines_parq a été ajouté")
    
    
    injonctions=f"""
        Create table injonctions AS 
        SELECT 
        reg_lb ,
        SUM(NB_SUITE) AS NB_INJONC
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE "Type de décision" = "Injonction"
        GROUP BY
        reg_lb """
    cursor.execute(injonctions)
    conn.commit()
    print("injonctions a été ajouté")
    
    prescriptions=f"""
        Create table prescriptions AS 
        SELECT 
        reg_lb ,
        SUM(NB_SUITE) AS NB_PRESCR
        FROM DWH_SUITES 
        --WHERE filtre = "Hors santé-environnement" 
        WHERE "Type de décision" = "Prescription"
        GROUP BY
        reg_lb"""
    cursor.execute(prescriptions)
    conn.commit()
    print("prescriptions a été ajouté")
    
    injonc_prescr=f"""
        Create table injonc_prescr AS 
        SELECT 
        reg_lb ,
        SUM(NB_SUITE) AS NB_INJONC_PRESCR
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE ("Type de décision" = "Injonction" OR "Type de décision" = "Prescription")
        GROUP BY
        reg_lb """
    cursor.execute(injonc_prescr)
    conn.commit()
    print("injonc_prescr a été ajouté")
    
    
    missions_real_tdb=f"""
        Create table missions_real_tdb 
        SELECT 
        reg_lb || statut_juridique_lb AS ID_REF ,
        reg_lb ,
        statut_juridique_lb ,
        SUM(NB_MISSION) AS NB_MISSIONS_REAL
        FROM DWH_MISSIONS
        --WHERE filtre = "Hors santé-environnement"
        GROUP BY 
        reg_lb ,
        statut_juridique_lb 
        ORDER BY 
        reg_lb ASC,
        statut_juridique_lb ASC"""
    cursor.execute(missions_real_tdb)
    conn.commit()
    print("missions_real_tdb a été ajouté")
    
    missions_clo_ss_s_tdb=f"""
        Create table missions_clo_ss_s_tdb AS (
        SELECT 
        reg_lb || statut_juridique_lb AS ID_REF ,
        reg_lb ,
        statut_juridique_lb ,
        COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSIONS_CLOTUREES_SANS_S
        FROM DWH_MISSIONS_SANCTION
        --WHERE filtre = "Hors santé-environnement" 
        WHERE SANCTION = "sans sanction"
        GROUP BY
        reg_lb ,
        statut_juridique_lb 
        ORDER BY 
        reg_lb ASC,
        statut_juridique_lb ASC"""
    cursor.execute(missions_clo_ss_s_tdb)
    conn.commit()
    print("missions_clo_ss_s_tdb a été ajouté")
    
    
    injonctions_tdb=f"""
        Create table injonctions_tdb AS 
        -- nombre d'injonctions
        SELECT 
        reg_lb || statut_juridique_lb AS ID_REF ,
        reg_lb ,
        statut_juridique_lb ,
        SUM(NB_SUITE) AS NB_INJONCTIONS
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE "Type de décision" = "Injonction"
        GROUP BY
        reg_lb ,
        statut_juridique_lb 
        ORDER BY 
        reg_lb ASC,
        statut_juridique_lb ASC"""
    cursor.execute(injonctions_tdb)
    conn.commit()
    print("injonctions_tdb a été ajouté")
    
    prescriptions_tdb=f"""
        Create table prescriptions_tdb AS 
        SELECT 
        reg_lb || statut_juridique_lb AS ID_REF ,
        reg_lb ,
        statut_juridique_lb ,
        SUM(NB_SUITE) AS NB_PRESCRIPTIONS
        FROM DWH_SUITES
        WHERE "Type de décision" = "Prescription"
        GROUP BY
        reg_lb ,
        statut_juridique_lb 
        ORDER BY 
        reg_lb ASC,
        statut_juridique_lb ASC"""
    cursor.execute(prescriptions_tdb)
    conn.commit()
    print("prescriptions_tdb a été ajouté")
    
    
    DWH_SUITES= f"""
        Create table DWH_SUITES AS
        SELECT *
        FROM cross_miss_sui_suites"""
    cursor.execute(DWH_SUITES)
    conn.commit()
    print("DWH_SUITES a été ajouté")
    
    
def select_tb_finale():
    dbname = read_settings("settings/settings.json", "db", "name")
    conn = sqlite3.connect(dbname + '.sqlite')
    cursor = conn.cursor()
    
    # Dictionnaire pour stocker les DataFrames
    tables_dict = {}
    
    # Creation de DWH_MISSIONS
    print ('Exécution requete DWH_MISSIONS')
    DWH_MISSIONS =f"""
        SELECT 
            reg_cd,
            reg_lb,
            dep_cd,
            dep_lb,
            com_cd,
            com_lb,
            finess_cd,
            Cible,
            statut_juridique_cd,
            statut_juridique_lb,
            "" AS groupe,
            type_de_mission,
            CTRL_PL_PI,
            "" AS filtre,
            "Statut de la mission",
            "Date réelle ""Visite"",
            "" AS GROUPE_2,
            groupe_siicea,
            "Type de planification",
            mission_conjointe,
            "Modalité de la mission",
            COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSION
            FROM missions_real_complet
            GROUP BY 
            reg_cd,
            reg_lb,
            dep_cd,
            dep_lb,
            com_cd,
            com_lb,
            finess_cd,
            Cible,
            statut_juridique_cd,
            statut_juridique_lb,
            groupe,
            type_de_mission,
            CTRL_PL_PI,
            filtre,
            "Statut de la mission",
            "Date réelle ""Visite"",
            GROUPE_2,
            groupe_siicea,
            "Type de planification",
            mission_conjointe,
            "Modalité de la mission" """
    cursor.execute(DWH_MISSIONS)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_MISSIONS= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_MISSIONS'] = DWH_MISSIONS
    print(DWH_MISSIONS)

     #Creation de DHW_MISSIONS par communes 
    print('Exécution de DWH_MISSIONS par communes')
    DWH_MISSIONS_AGG_COMMUNES= f"""
    Select * 
    from communes """
    cursor.execute(DWH_MISSIONS_AGG_COMMUNES)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_MISSIONS_AGG_COMMUNES= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_MISSIONS_AGG_COMMUNES'] = DWH_MISSIONS_AGG_COMMUNES
    print(DWH_MISSIONS_AGG_COMMUNES)  
    
    
    #Creation de DHW_MISSIONS par departements
    print('Exécution de DWH_MISSIONS par departements')
    DWH_MISSIONS_AGG_departement= f"""
    Select * 
    from departements """
    cursor.execute(DWH_MISSIONS_AGG_departement)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_MISSIONS_AGG_departement= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_MISSIONS_AGG_departement'] = DWH_MISSIONS_AGG_departement
    print(DWH_MISSIONS_AGG_departement)  
    
    
    #Creation de DHW_MISSIONS par regions
    print('Exécution de DWH_MISSIONS par regions')
    DWH_MISSIONS_AGG_region= f"""
    Select * 
    from region """
    cursor.execute(DWH_MISSIONS_AGG_region)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_MISSIONS_AGG_region= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_MISSIONS_AGG_region'] = DWH_MISSIONS_AGG_region
    print(DWH_MISSIONS_AGG_region) 
    
    
    #Creation de DHW_MISSIONS_PROG 
    print('Exécution de DWH_MISSIONS_PROG')
    DWH_MISSIONS_PROG= f"""
        SELECT 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        statut_juridique_cd,
        statut_juridique_lb,
        "" AS groupe,
        type_de_mission,
        CTRL_PL_PI,
        "" AS filtre,
        "Statut de la mission",
        "Date provisoire ""Visite"",
        "" AS GROUPE_2,
        groupe_siicea,
        "Type de planification",
        mission_conjointe,
        "Modalité de la mission",
        COUNT(DISTINCT "Identifiant de la mission") AS NB_MISSION
        FROM missions_prev_complet
        GROUP BY 
        reg_cd,
        reg_lb,
        dep_cd,
        dep_lb,
        com_cd,
        com_lb,
        finess_cd,
        Cible,
        statut_juridique_cd,
        statut_juridique_lb,
        groupe,
        type_de_mission,
        CTRL_PL_PI,
        filtre,
        "Statut de la mission",
        "Date provisoire ""Visite"",
        GROUPE_2,
        groupe_siicea,
        "Type de planification",
        mission_conjointe,
        "Modalité de la mission" """
    cursor.execute(DWH_MISSIONS_PROG)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_MISSIONS_PROG= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_MISSIONS_PROG'] = DWH_MISSIONS_PROG
    print(DWH_MISSIONS_PROG) 
    
    #Creation de DHW_MISSIONS_SANCTION 
    print('Exécution de DWH_MISSIONS_SANCTION')
    DHW_MISSIONS_SANCTION= f"""
        SELECT 
        missions_real_complet.*,
        COALESCE(contrainte.AVEC_SANCTION, 'sans sanction') AS SANCTION
        FROM missions_real_complet
        LEFT JOIN contrainte ON missions_real_complet."Identifiant de la mission" = contrainte."Identifiant de la mission"
        WHERE missions_real_complet."Statut de la mission" = "Clôturé" """
    cursor.execute(DHW_MISSIONS_SANCTION)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DHW_MISSIONS_SANCTION= pd.DataFrame(res,columns=columns)
    tables_dict['DHW_MISSIONS_SANCTION'] = DHW_MISSIONS_SANCTION
    print(DHW_MISSIONS_SANCTION) 
    
    #Creation de DHW_SUITES 
    print('Exécution de DWH_SUITES')
    DWH_SUITES= f"""
        SELECT *
        FROM cross_miss_sui_suites"""
    cursor.execute(DWH_SUITES)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    DWH_SUITES= pd.DataFrame(res,columns=columns)
    tables_dict['DWH_SUITES'] = DWH_SUITES
    print(DWH_SUITES) 
    
    #Creation de TDB_INJONCTIONS
    print('Exécution de TDB_INJONCTIONS')
    TDB_INJONCTIONS= f"""
        SELECT 
        reg_lb AS "Région",
        statut_juridique_lb AS "Statut juridique",
        "Thème Décision" ,
        "Sous-thème Décision" ,
        SUM(NB_SUITE) AS "Injonctions"
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE "Type de décision" = "Injonction"
        GROUP BY
        reg_lb ,
        statut_juridique_lb,
        "Thème Décision" ,
        "Sous-thème Décision" """
    cursor.execute(TDB_INJONCTIONS)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    TDB_INJONCTIONS= pd.DataFrame(res,columns=columns)
    tables_dict['TDB_INJONCTIONS'] = TDB_INJONCTIONS
    print(TDB_INJONCTIONS) 
    
    
    #Creation de TDB_INJONCTIONS_PRESCRIPTIONS
    print('Exécution de TDB_INJONCTIONS_PRESCRIPTIONS')
    TDB_INJONCTIONS_PRESCRIPTIONS= f"""
        SELECT
        reg_lb AS "Région",
        statut_juridique_lb AS "Statut juridique",
        "Thème Décision" ,
        "Sous-thème Décision" ,
        SUM(NB_SUITE) AS "Injonctions + prescriptions"
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE ("Type de décision" = "Injonction" OR "Type de décision" = "Prescription")
        GROUP BY
        reg_lb ,
        statut_juridique_lb,
        "Thème Décision" ,
        "Sous-thème Décision" """
    cursor.execute(TDB_INJONCTIONS_PRESCRIPTIONS)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    TDB_INJONCTIONS_PRESCRIPTIONS= pd.DataFrame(res,columns=columns)
    tables_dict['TDB_INJONCTIONS_PRESCRIPTIONS'] = TDB_INJONCTIONS_PRESCRIPTIONS
    print(TDB_INJONCTIONS_PRESCRIPTIONS) 
    
    
    #Creation de TDB_MISSIONS_SANCTIONS
    print('Exécution de TDB_MISSIONS_SANCTIONS')
    TDB_MISSIONS_SANCTIONS= f"""
    SELECT 
        ehpad_control.reg_lb AS "Région",
        NB_ETAB_CONTROLE AS "Nombre d'EHPAD différents contrôlés",
        NB_ETAB_CONTROLE_NB_EHPAD AS "Taux d'EHPAD différents contrôlés (en %)",
        NB_MISSIONS AS "Nombre d'I-C d'EHPAD réalisées",
        NB_MISSIONS_CLOTUREES AS "Nombre d'I-C clôturées",
        NB_MISSIONS_CLOTUREES_SANS_S AS "Nombre d'I-C clôturées sans suites coercitives (injonction, prescription) ni saisine",
        -- Taux d'I-C clôturées sans suites coercitives (injonction, prescription) ni saisine (en %)
        (CAST(NB_MISSIONS_CLOTUREES_SANS_S AS FLOAT) / CAST(NB_MISSIONS_CLOTUREES AS FLOAT))*100 AS "Taux d'I-C clôturées sans suites coercitives (injonction, prescription) ni saisine (en %)",
        NB_SAISINES_PARQUET AS "Nombre de signalements au Parquet effectués (art. 40 CPP)" ,
        NB_INJONC AS "Nbr total injonctions",
        (CAST(NB_INJONC AS FLOAT) / CAST(NB_MISSIONS AS FLOAT)) AS "Nbr injonctions moyen / I-C réalisé" ,
        NB_PRESCR AS "Nbr total prescriptions",
        (CAST(NB_PRESCR AS FLOAT) / CAST(NB_MISSIONS AS FLOAT)) AS "Nbr prescriptions moyen / I-C réalisé" ,
        NB_INJONC_PRESCR AS "Nbr total injonctions + prescriptions" ,
        (CAST(NB_INJONC_PRESCR AS FLOAT) / CAST(NB_MISSIONS AS FLOAT)) AS "Nbr injonctions et prescriptions moyen par I-C réalisé"
        FROM ehpad_control
        LEFT JOIN missions_real_dwh ON ehpad_control.reg_lb = missions_real_dwh.reg_lb
        LEFT JOIN missions_clot ON ehpad_control.reg_lb = missions_clot.reg_lb
        LEFT JOIN missions_clo_ss_s ON ehpad_control.reg_lb = missions_clo_ss_s.reg_lb
        LEFT JOIN saisines_parq ON ehpad_control.reg_lb = saisines_parq.reg_lb
        LEFT JOIN injonctions ON ehpad_control.reg_lb = injonctions.reg_lb
        LEFT JOIN prescriptions ON ehpad_control.reg_lb = prescriptions.reg_lb
        LEFT JOIN injonc_prescr ON ehpad_control.reg_lb = injonc_prescr.reg_lb """
    cursor.execute(TDB_MISSIONS_SANCTIONS)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    TDB_MISSIONS_SANCTIONS= pd.DataFrame(res,columns=columns)
    tables_dict['TDB_MISSIONS_SANCTIONS'] = TDB_MISSIONS_SANCTIONS
    print(TDB_MISSIONS_SANCTIONS) 
    
    
    #Creation de TDB_PRESCRIPTION
    print('Exécution de TDB_PRESCRIPTION')
    TDB_PRESCRIPTION= f"""
        SELECT
        reg_lb AS "Région",
        statut_juridique_lb AS "Statut juridique",
        "Thème Décision" ,
        "Sous-thème Décision" ,
        SUM(NB_SUITE) AS "Precriptions"
        FROM DWH_SUITES
        --WHERE filtre = "Hors santé-environnement" 
        WHERE "Type de décision" = "Prescription"
        GROUP BY
        reg_lb ,
        statut_juridique_lb,
        "Thème Décision" ,
        "Sous-thème Décision" """
    cursor.execute(TDB_PRESCRIPTION)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    TDB_PRESCRIPTION= pd.DataFrame(res,columns=columns)
    tables_dict['TDB_PRESCRIPTION'] = TDB_PRESCRIPTION
    print(TDB_PRESCRIPTION) 
    
    
    #Creation de TDB_SANCTION_STATUT_JURIDIQUE
    print('Exécution de TDB_SANCTION_STATUT_JURIDIQUE')
    TDB_SANCTION_STATUT_JURIDIQUE= f"""
        SELECT 
        missions_real_tdb.reg_lb AS "Région",
        missions_real_tdb.statut_juridique_lb AS "Statut juridique",
        NB_MISSIONS_REAL AS "I-C d'EHPAD réalisées",
        NB_MISSIONS_CLOTUREES_SANS_S AS "Nombre d'I-C clôturées sans suites coercitives (injonction, prescription) ni saisine",
        NB_INJONCTIONS AS "Total injonctions",
        CAST(NB_INJONCTIONS AS FLOAT) / CAST(NB_MISSIONS_REAL AS FLOAT) AS "Nombre moyen d'injonctions / I-C réalisé",
        NB_PRESCRIPTIONS AS "Total prescriptions",
        CAST(NB_PRESCRIPTIONS AS FLOAT) / CAST(NB_MISSIONS_REAL AS FLOAT) AS "Nombre moyen de prescriptions / I-C réalisé",
        NB_INJONCTIONS + NB_PRESCRIPTIONS AS "Total injonctions et prescriptions",
        CAST((NB_INJONCTIONS + NB_PRESCRIPTIONS) AS FLOAT) / CAST(NB_MISSIONS_REAL AS FLOAT) AS "Nombre moyen d'injonctions et de prescriptions / I-C réalisé",
        (CAST(NB_INJONCTIONS AS FLOAT) / CAST((NB_INJONCTIONS + NB_PRESCRIPTIONS) AS FLOAT))*100 AS "Part injonctions (en %)",
        (CAST(NB_PRESCRIPTIONS AS FLOAT) / CAST((NB_INJONCTIONS + NB_PRESCRIPTIONS) AS FLOAT))*100 AS "Part prescriptions (en %)"
        FROM missions_real_tdb
        LEFT JOIN missions_clo_ss_s_tdb ON missions_real_tdb.ID_REF = missions_clo_ss_s_tdb.ID_REF
        LEFT JOIN injonctions_tdb ON missions_real_tdb.ID_REF = injonctions_tdb.ID_REF
        LEFT JOIN prescriptions_tdb ON missions_real_tdb.ID_REF = prescriptions_tdb.ID_REF """
    cursor.execute(TDB_SANCTION_STATUT_JURIDIQUE)
    res=cursor.fetchall()
    columns= [col[0] for col in cursor.description]
    TDB_SANCTION_STATUT_JURIDIQUE= pd.DataFrame(res,columns=columns)
    tables_dict['TDB_SANCTION_STATUT_JURIDIQUE'] = TDB_SANCTION_STATUT_JURIDIQUE
    print(TDB_SANCTION_STATUT_JURIDIQUE) 
    

    conn.close()
    return tables_dict
    
    
    
    
    
    
    
    
    
    
      

