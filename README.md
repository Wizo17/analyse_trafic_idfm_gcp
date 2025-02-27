# Ile de France Mobilité traffic analysis

The project involves analysis using data from Ile-de-France Mobility. 


## Build with

The project uses:
* [Python](https://www.python.org/)
* [PySpark, SparkSQL](https://spark.apache.org/docs/latest/api/python/index.html)
* [Google Cloud Platform (Dataproc, BigQuery, Cloud Run, Cloud Storage, Artifactory Registry)](https://cloud.google.com/?hl=fr)
* [Shell script](https://www.shellscript.sh/)
* [Postgres](https://www.postgresql.org/)
* [Docker](https://www.docker.com/)
* [Looker Studio](https://lookerstudio.google.com/)

Ressources uses:
* [IDFM GTFS - DATA IDFM](https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/information/)
* [IDFM GTFS - DATA GOUV](https://www.data.gouv.fr/fr/datasets/horaires-prevus-sur-les-lignes-de-transport-en-commun-dile-de-france-gtfs-datahub/)


## How it works
* Upload a zip to a <u>Cloud Storage</u> bucket.
* Sends an <u>Eventarc</u> event to Cloud Run.
* <u>Cloud Run</u> uses a docker image with [cloud_run_functions_etl.py](gcp/cloud_run_functions_etl.py) and [cloud_run_functions_etl.sh](gcp/cloud_run_functions_etl.sh).
* <u>Docker</u> file is [here](gcp/Dockerfile).
* Creation of a <u>Dataproc</u> cluster.
* Sending a <u>PySpark job</u> to the Dataproc cluster.
* Feed <u>BigQuery</u> tables after the ETL process.
* If all goes well, Dataproc cluster and files are deleted.
* Views are built from data in BigQuery.
* Dashboard creation with <u>Looker Studio</u>.


## Install

There are 2 choices for running the programs. Either with postgres locally or on the cloud. Or with a GCP environment.

### Requirements

* Python
* Spark
* Postgres (on site)
* GCP Account (cloud)
* Git
* JDBC Postgres (on site)

### On site

1. Pull git repository.
2. Run: `cd analyse_trafic_idfm_gcp`.
3. If unix or macos then run `chmod +x setup_env.sh; ./setup_env.sh` else `setup_env.bat`.
4. Run: `mkdir data` if data folder not exist.
5. Download the data to Data IDFM or Data Gouv, then create a folder in data and unzip the files in it.
6. Download [JDBC Postgres](https://jdbc.postgresql.org/download/postgresql-42.7.5.jar) and copy it to a folder of your choice
7. In the `.env` file, if you're on **local**, put **local**.
8. Update the `.env` file according to your environment.
9. Run: `python src/main.py "yyyy-MM-dd"`, replace yyyy-MM-dd by your date ingestion.

### GCP

1. Have a GCP account or create an account with a free trial product.
2. Create a new project (analytics-trafic-idfm).
3. Use the console to create elements.
4. Follow script instructions [init_gcp_environment.sh](gcp/init_gcp_environment.sh) to create only your own elements.
5. **!!!Attention!!!**, you don't need to run the whole script and you need to update the commands with your own information.
6. You can do all the data analysis you want with the data.
7. Sample queries can be found in the gcp/bigquery folder.
8. A sample report can be found [here](gcp/report/Analysis_trafic_IDFM.pdf) 

## Versions
**LTS :** [1.0]((https://github.com/Wizo17/analyse_trafic_idfm_gcp.git))

## Auteurs
* [@wizo17](https://github.com/Wizo17)

## License
This project is licensed by  ``MIT`` - see [LICENSE](LICENSE) for more informations.

