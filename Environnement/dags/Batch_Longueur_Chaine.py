from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator

import pandas as pd
import numpy as np

import glob
import os


# =============================================================================
# Initilisation variable globales
# =============================================================================

chemin_resultats = "/opt/airflow/dags/resultats/"
chemin_donnees = "/opt/airflow/dags/donnees/"
PATRONYMES = f"{chemin_donnees}patronymes.csv"

NB_TRAITEMENTS = 5
NROWS = 800000
CHUNKSIZE = NROWS // NB_TRAITEMENTS


# =============================================================================
# Test BD Mysql
# =============================================================================

requete_creer = """DROP TABLE IF EXISTS PATRONYME; 
                   CREATE TABLE PATRONYME (
                   id_patronyme INT AUTO_INCREMENT PRIMARY KEY, 
                   patronyme VARCHAR(30) NOT NULL, 
                   nombre INT NOT NULL)"""


donnee = pd.read_csv(PATRONYMES, sep=",")
requete_inserer = ""


for i in range(1000):
    patronyme, nombre = donnee.iloc[i]
    requete_inserer += f"""INSERT INTO PATRONYME(patronyme, nombre) 
                           VALUES(
                           "{patronyme}",
                            {nombre}
                            ); """


# =============================================================================
# Fonctions non DAG
# =============================================================================

def inserer_taille(lot):
    """
    Insère une nouvelle colonne 'Taille" dans un dataset correspondant à
    la longueur d'un patronyme.
    :param lot: dataset à modifier
    :return: le dataset contenant la nouvelle colonne et les informations de longueur
    """

    donnees = lot.copy().fillna("")
    nb_donnees = donnees.shape[0]
    patronymes = donnees.patronyme.values
    colonne_tailles = np.zeros(nb_donnees, dtype=int)

    for p in range(nb_donnees):
        colonne_tailles[p] = len(patronymes[p])

    donnees = donnees.assign(Taille=colonne_tailles)

    return donnees


def recuperer_fichiers():
    """
    Récupère les fichiers patronymes créés
    :return: la liste des chemins des fichiers
    """

    return glob.glob(f"{chemin_resultats}*-*.csv")


# =============================================================================
# Fonctions liées aux taâches
# =============================================================================

def preparer_data():
    """
    Initialise les données de la table avec création de la colonne Taille.
    :return: None
    """

    # Suppression d'anciens fichiers patronymes avec colonne Tailles
    try:
        os.remove(chemin_resultats + "*")
    except OSError:
        pass

    print("PREPARER_DATA")


def traitement_unitaire_b(**kwargs):
    """
    Calcule, insère les tailles et écrit les nouvelles données dans des fichiers distincts.
    :param kwargs: {lot}
    :return: None
    """

    lot = kwargs["lot"]
    donnees = inserer_taille(lot)
    # Header = true si premier lot
    donnees.to_csv(f"{chemin_resultats}patronymes_tailles-{lot.index[0]}.csv", header=(lot.index[0] == 0), index=False)
    print(f"TRAITEMENT_UNITAIRE_BATCH_LOT_{lot.index[0]}")


def concatener_data_test():
    """
    Concatène les nouveaux fichiers patronymes creés.
    :return: None
    """

    fichiers = recuperer_fichiers()
    with open(f"{chemin_resultats}patronymes_tailles.csv", 'a') as f_final:
        for fichier in fichiers:
            with open(fichier, 'r') as f:
                f_final.write(f.read())

    print("CONCATENER_DATA_TEST")


def effacer_data():
    """
    Efface tous les fichiers CSV temporaire créés.
    :return: None
    """

    fichiers = recuperer_fichiers()

    if fichiers:
        try:
            for fichier in fichiers:
                os.remove(fichier)
        except OSError:
            pass

    print("EFFACER_DATA")


# =============================================================================
# DAG et TASKs
# =============================================================================

dag = DAG(
    dag_id='Batch_Longueur_Chaine',
    start_date=days_ago(2)
)

ceer_table = MySqlOperator(
    task_id='TEST_creer_table',
    sql=requete_creer,
    mysql_conn_id='mysql_connexion',
    database='airflow',
    autocommit=True,
    dag=dag
)

inserer_table = MySqlOperator(
    task_id='TEST_inserer_table',
    sql=requete_inserer,
    mysql_conn_id='mysql_connexion',
    database='airflow',
    autocommit=True,
    dag=dag
)

preparer_data = PythonOperator(
    task_id='preparer_data',
    python_callable=preparer_data,
    dag=dag,
)

concatener_data = BashOperator(
    task_id='concatener_data',
    bash_command=f"cat {chemin_resultats}*-*.csv >> {chemin_resultats}patronymes_tailles.csv",
    dag=dag,
)

effacer_data = PythonOperator(
    task_id='effacer_data',
    python_callable=effacer_data,
    dag=dag,
)

for lot in pd.read_csv(PATRONYMES, sep=",", chunksize=CHUNKSIZE, nrows=NROWS):
    traitement_unitaire_batch = PythonOperator(
        task_id=f"traitement_unitaire_batch_{lot.index[0]}",
        python_callable=traitement_unitaire_b,
        op_kwargs={'lot': lot},
        dag=dag
    )

    ceer_table >> inserer_table >> preparer_data >> traitement_unitaire_batch >> concatener_data >> effacer_data
