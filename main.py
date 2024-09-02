import json
import argparse
import re
import os
import pandas as pd
from Modules.init_db.init_db import connDb
from Modules.utils.utils import convertXlsxToCsv, read_settings, renameFilesByPattern, createSaCiblesTable
from Modules.transform.transform import select_tb_finale, inittable

def main(args):
    if args.commande == "import":
        importData()
    elif args.commande == "create_csv":
        createCsv()
    elif args.commande == "init_database":
        exeDbInit()
    elif args.commande == "load_csv":
        loadCsvToDb()
    elif args.commande == "transform":
        transform()
    elif args.commande == "export":
        createExport()
    elif args.commande == "all":
        allFunctions()
    return

def exeDbInit():
    dbname = read_settings('settings/settings.json', "db", "name")
    conn = connDb(dbname)
    conn.close()
    return


def createCsv():
    input_dir = 'data/input'
    # Appel de la fonction pour renommer les fichiers dans le dossier input
    renameFilesByPattern(input_dir)

    all_files = os.listdir(input_dir)
    for input_file_name in all_files:
        input_file_path = os.path.join(input_dir, input_file_name)
        output_file_path = os.path.join('data/to_csv', input_file_name.split('.')[0] + '.csv')

        if re.search(r'demo\.csv|demo\.xlsx', input_file_name):
            print('file demo not added')
        elif input_file_name.split('.')[-1].lower() == 'xlsx':
            convertXlsxToCsv(input_file_path, output_file_path)
            print(f'converted excel file and added: {input_file_name}')
        elif input_file_name.split('.')[-1].lower() == 'csv':
            output_excel = input_file_path.split('.')[0] + '.xlsx'
            try:
                df = pd.read_csv(input_file_path, sep=';', encoding='latin-1', low_memory=False)
                df.to_excel(output_excel, index=None, header=True)
                df2 = pd.read_excel(output_excel)
                df2.to_csv(output_file_path, index=None, header=True, sep=';', encoding='UTF-8')
                print(f'added csv file: {input_file_name}')
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier {input_file_name}: {e}")


def loadCsvToDb():
    # Lire le nom de la base de données depuis le fichier de configuration
    dbname = read_settings('settings/settings.json', "db", "name")
    # Se connecter à la base de données
    conn = connDb(dbname)
    if conn is None:
        print("Échec de la connexion à la base de données")
        return
    # Lister tous les fichiers CSV dans le répertoire 'data/to_csv'
    allCsv = os.listdir('data/to_csv')
    # Parcourir chaque fichier CSV
    for inputCsvFileName in allCsv:
        inputCsvFilePath = os.path.join('data/to_csv', inputCsvFileName)
        table_name = inputCsvFileName.split('.')[0]
        
        # Appeler la création de la table sa_cibles si c'est le fichier approprié
        if table_name == 'sa_cibles':  # Vérifie que c'est le bon fichier
            createSaCiblesTable(dbname, inputCsvFilePath)
        else:
            # Charger les autres fichiers CSV en DataFrame
            df = pd.read_csv(inputCsvFilePath, sep=';', encoding='UTF-8', low_memory=False)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"La table {table_name} a été chargée dans la base de données.")
    conn.close()
    return


def transform():
    dbname = read_settings("settings/settings.json", "db", "name")
    conn = connDb(dbname)
    if conn is None:
        print("Échec de la connexion à la base de données")
        return
    inittable(conn)
    print("Table initialisée avec succès.")
    select_tb_finale()
    print("Transformation exécutée")
    conn.close()
    
def createExport():
    # Appel de la fonction select_tb_finale pour récupérer les tables
    tables_dict = select_tb_finale()
    # Créer le répertoire de sortie s'il n'existe pas
    output_dir = 'data/output'
    os.makedirs(output_dir, exist_ok=True)
    # Exporter chaque DataFrame dans un fichier Excel distinct
    for table_name, df in tables_dict.items():
        output_file_path = os.path.join(output_dir, f"{table_name}.xlsx")
        df.to_excel(output_file_path, index=False)
        print(f"Table {table_name} exportée vers {output_file_path}")


def allFunctions():
    exeDbInit()
    #createCsv()
    #loadCsvToDb()
    transform()
    #createExport()
    return

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
