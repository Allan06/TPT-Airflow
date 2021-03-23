from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np

import glob
import os


# =============================================================================
# Fonctions non DAG
# =============================================================================

class Table:
    """
    Classe définissant les fonctionnalité d'une table de données.
    """

    def __init__(self):
        self.donnees = None
        self.nom_fichier = ""
        self.nb_lignes = 0

    def charger_donnees(self, nom_fichier, sep=',', nrows=None):
        """
        Charge les données d'un fihier dans un dataset
        :param nom_fichier: chemin du fichier à charger
        :param sep: type de séparateur
        :param nrows: nombre de ligne à charger
        :return: None
        """

        self.nom_fichier = nom_fichier
        self.donnees = pd.read_csv(nom_fichier, sep=sep, nrows=nrows)
        self.nb_lignes =  self.donnees.shape[0]


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

    return glob.glob('/opt/airflow/dags/Resultats/*-*.csv')


# =============================================================================
# Initilisation variable globales
# =============================================================================

PATRONYMES = "/opt/airflow/dags/patronymes.csv"

NB_TRAITEMENTS = 5
NROWS = 800000
CHUNKSIZE = NROWS // NB_TRAITEMENTS

table = Table()


# =============================================================================
# Fonctions et DAG
# =============================================================================

def preparer_data(**kwargs):
    """
    Initialise les données de la table avec création de la colonne Taille.
    :param kwargs: {fichier, chunksize, nrows}
    :return: None
    """

    global table

    # Récupération des paramètres
    fichier = kwargs["fichier"]
    chunksize = kwargs["chunksize"]
    nrows = kwargs["nrows"]

    # Initialisation de la table
    table.charger_donnees(fichier, sep=",", nrows=nrows)
    colonne_tailles = np.zeros(table.nb_lignes, dtype=int)
    table.donnees.assign(Taille=colonne_tailles)

    # Suppression d'anciens fichiers patronymes avec colonne Tailles
    try:
        os.remove("/opt/airflow/dags/patronymes_tailles.csv")
    except OSError:
        pass

    print(f"preparer {table.nb_lignes} {table.nom_fichier}")


def traitement(**kwargs):
    """
    Calcule, insère les tailles et écrit les nouvelles données dans des fichiers distincts.
    :param kwargs: {lot}
    :return: None
    """

    lot = kwargs["lot"]
    donnees = inserer_taille(lot)
    # Header = true si premier lot
    donnees.to_csv(f'/opt/airflow/dags/Resultats/patronymes_tailles-{lot.index[0]}.csv', header=False, index=False)

    # TODO : Traitement à terminer
    #  ================================================================
    #  Ajouter une colonne de comptage des longueur des nom patrymoniale
    #  Enregistrer le traitement dans un fichier
    #  Concatener les 3 fichiers de sortie de traitements.
    #  Tous les élement doivent être envoyé dans un fichier en mode append
    #  Essayer avec un plus gros fichier et voir le test avec une base


def concatener_data():
    """
    Concatène les nouveaux fichiers patronymes creés.
    :return: None
    """

    fichiers = recuperer_fichiers()
    with open('/opt/airflow/dags/patronymes_tailles.csv', 'a') as f_final:
        for fichier in fichiers:
            with open(fichier, 'r') as f:
                f_final.write(f.read())


def maj_table():
    """
    Met à jour la table de données avec les nouvelles données.
    :return: None
    """

    global table

    donnees_copie = pd.read_csv('/opt/airflow/dags/patronymes_tailles.csv', sep=",")
    for ligne in range(donnees_copie.shape[0]):
        table.donnees.loc[ligne, "Taille"] = donnees_copie.loc[ligne, "Taille"]

    print("Mise à jour table")
    print(table.donnees.values)


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

    print("effacer_data")


dag = DAG(
    dag_id='Batch_Longueur_Chaine',
    start_date=days_ago(2)
)

preparer_data = PythonOperator(
    task_id='preparer_data',
    python_callable=preparer_data,
    op_kwargs={"fichier": PATRONYMES, "chunksize": CHUNKSIZE, "nrows": NROWS},
    dag=dag,
)

concatener_data = BashOperator(
    task_id='concatener_data',
    bash_command='cat /opt/airflow/dags/Resultats/*.csv >> /opt/airflow/dags/patronymes_tailles.csv',
    dag=dag,
)
"""
maj_table = PythonOperator(
    task_id='maj_table',
    python_callable=maj_table,
    dag=dag,
)
"""

effacer_data = PythonOperator(
    task_id='effacer_data',
    python_callable=effacer_data,
    dag=dag,
)


for lot in pd.read_csv(PATRONYMES, sep=",", chunksize=CHUNKSIZE, nrows=NROWS):
    traitement_unitaire_batch = PythonOperator(
        task_id='traitement_unitaire_batch_' + str(lot.index[0]),
        python_callable=traitement,
        op_kwargs={'lot': lot},
        dag=dag,
    )
    preparer_data >> traitement_unitaire_batch >> concatener_data >> effacer_data


