import os
import pandas as pd
import json
import logging
import re
import sqlite3

def checkIfPathExists(file):
    if os.path.exists(file):
        os.remove(file)
        print('Ancien fichier écrasé')

def convertXlsxToCsv(inputExcelFilePath, outputCsvFilePath):
    try:
        excelFile = pd.read_excel(inputExcelFilePath, header=0)
        checkIfPathExists(outputCsvFilePath)
        excelFile.to_csv(outputCsvFilePath, index=None, header=True, sep=';', encoding='UTF-8')
        return outputCsvFilePath
    except ValueError as err:
        print(err)
        return str(err)

def csvReader(csvFilePath):
    df = pd.read_csv(csvFilePath, sep=';', encoding='UTF-8', low_memory=False)
    return df

def read_settings(path_in, dict_key, elem):
    """
    Permet de lire le document settings et retourne les informations souhaitées au format dictionnaire.
    Paramètres :
        - path_in : Chemin du dossier settings où sont stockées les informations.
        - dict_key : Clé dans le dictionnaire principal.
        - elem : Élément au sein du dictionnaire dont on souhaite retourner les informations.
    """
    with open(path_in) as f:
        dict_ret = json.load(f)
    
    # Vérifier si dict_ret est un dictionnaire
    if isinstance(dict_ret, dict):
        # Obtenir la liste correspondant à la clé demandée
        dict_list = dict_ret.get(dict_key, [])
        
        # Assurer que c'est une liste et prendre le premier élément s'il existe
        if isinstance(dict_list, list) and len(dict_list) > 0:
            dict_ret = dict_list[0]
        else:
            dict_ret = {}
    else:
        dict_ret = {}
    
    param_config = dict_ret.get(elem, {})
    
    logging.info(f"Lecture param config {path_in}.")
    return param_config


def renameFilesByPattern(input_dir):
    """
    Renomme les fichiers du dossier en fonction de motifs définis.

    :param input_dir: Chemin du dossier contenant les fichiers à renommer.
    """
    # Définir les règles de renommage (ancien motif -> nouveau motif)
    rename_rules = {
        r"Décisions ESMS 2021 22 23 24": "sa_decisions",
        r"Groupes-cibles-19-07-2024_Cibles": "sa_cibles",
        r"Missions ESMS 2021 22 23 24 au 19 07 24_Missions prev": "sa_missions_prev",
        r"Missions ESMS 2021 22 23 24 au 19 07 24_Missions": "sa_missions"
    }

    try:
        # Liste tous les fichiers dans le répertoire spécifié
        all_files = os.listdir(input_dir)

        # Parcourt chaque fichier dans le dossier
        for file_name in all_files:
            old_path = os.path.join(input_dir, file_name)

            # Vérifie chaque règle de renommage
            for old_pattern, new_pattern in rename_rules.items():
                # Vérifie si le nom du fichier correspond au motif
                if re.search(old_pattern, file_name):
                    new_name = re.sub(old_pattern, new_pattern, file_name)  # Applique le renommage
                    new_path = os.path.join(input_dir, new_name)

                    # Renomme le fichier
                    os.rename(old_path, new_path)
                    print(f"Renommé : {file_name} -> {new_name}")
                    break  # Sort de la boucle après le premier renommage pour éviter plusieurs remplacements
                else:
                    print(f"Aucun changement pour : {file_name}")
    except Exception as e:
        print(f"Erreur lors du renommage des fichiers : {e}")
        

def createSaCiblesTable(dbname, csv_file_path):

    # Connexion à la base de données
    conn = sqlite3.connect(dbname)
    cursor = conn.cursor()
    
    # Création de la table sa_cibles (modifie les types de colonnes si nécessaire)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sa_cibles (
        "Unnamed: 0"
        FINESS
        RPPSS
        SIRET
        "Code APE"
        "Code UAI"
        "Groupe de cibles"
        Nom
        Departement
        Commune
        Adresse
        "Nombre de missions rÃ©alisÃ©es" 
        "Nombre de missions abondonnÃ©es / reportÃ©es"
        "Nombre de dÃ©cisions prises"
        )
        """)
    
    # Lecture du fichier CSV
    df = pd.read_csv(csv_file_path, sep=';', encoding='UTF-8', low_memory=False)
    
    # Charger les données dans la table
    df.to_sql('sa_cibles', conn, if_exists='replace', index=False)
    
    # Confirmation et fermeture de la connexion
    print("La table `sa_cibles` a été créée et les données ont été chargées.")
    conn.commit()
    conn.close()


