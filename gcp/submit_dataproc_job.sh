#!/bin/bash

echo "Retrieving the git repository..."
git clone https://github.com/Wizo17/analyse_trafic_idfm_gcp.git

if [ $? -eq 0 ]; then
    cd analyse_trafic_idfm_gcp

    cp .env_prod .env

    if [ -f .env ]; then
        source .env
        date_var=$(date +%F)

        gcloud dataproc jobs submit pyspark \
        --cluster $GCP_CLUSTER_DATAPROC_NAME \
        --region $GCP_REGION_DEFAULT \
        src/main.py "$date_var"

        if [ $? -eq 0 ]; then
            # gsutil rm "$GCP_BUCKET_NAME/*"
            exit 0
        else
            echo "Spark job execution failed!"
            exit 1
        fi
    else
        echo ".env file not found!"
        exit 1
    fi

else
    echo "An error occurred while cloning the repository..."
    exit 1
fi

