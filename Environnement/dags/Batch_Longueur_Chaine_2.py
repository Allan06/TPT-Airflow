from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import numpy as np
import pandas as pd
import os

PATRONYMES = "/opt/airflow/dags/patronymes.csv"

NB_TRAITEMENTS = 25
NROWS = 800000
CHUNKSIZE = NROWS // NB_TRAITEMENTS


def prepare_data():
    print("prepare_data")
    try:
        os.remove('/opt/airflow/dags/patronymes_tailles.csv')
    except OSError:
        pass


def effacer_data():
    print("effacer_data")


def traitement(**kwargs):

    lot = kwargs["lot"]
    donnees = inserer_taille(lot)
    donnees.to_csv('/opt/airflow/dags/patronymes_tailles.csv', mode='a', header=False)

    # TODO : Traitement à terminer
    #  ================================================================
    #  Ajouter une colonne de comptage des longueur des nom patrymoniale
    #  Enregistrer le traitement dans un fichier
    #  Concatener les 3 fichiers de sortie de traitements.
    #  Tous les élement doivent être envoyé dans un fichier en mode append


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


dag = DAG(
    dag_id='Batch_Longueur_Chaine_2',
    start_date=days_ago(2)
)

preparer_data = PythonOperator(
    task_id='preparer_data',
    python_callable=prepare_data,
    dag=dag,
)

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

    preparer_data >> traitement_unitaire_batch >> effacer_data
