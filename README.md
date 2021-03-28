# TPT - Apache/Airflow

## Initialisation
---

### Création de l'environnement

	-	docker-compose -f docker-compose.yaml up

### Terminal Airflow

	-   Par le nom du container : 
        -> docker exec -it environnement_airflow-webserver_1 /bin/bash
	-	Par l'ID du container :
        -> docker ps (afficher les IDs des container actifs) 
	    -> docker exec -it "container_ID" /bin/bash

### Si Warning Microsof Azure, taper dans le terminal airflow

	-	pip uninstall --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

## Outils de management
---

Parametrer la base de données MySQL :

Dans le Webservr d'Airflow:

    - Admin -> Connections -> Add a new record
        - Conn Id   = mysql
        - Conn Type = MySQL
        - Host      = mysql
        - Schema    = airflow
        - Login     = root
        - Password  = root
        - Port      = 3306


Gestion de l'environnement :
-   [VS Code](https://code.visualstudio.com/)

        -> Docker (Plugin gestion fichiers des containers)
        -> SQLTools + MySQL/MariaDB DRIVER (Plugin SGBD)

-   [MySQL Workbench](https://dev.mysql.com/downloads/workbench/) 
        
        -> MySQL
        -> MariaDB

-   [PgAdmin](https://www.pgadmin.org/)

        -> PostgreSQL

