#!/bin/bash


# ----------------------------------------------------------------------------
# Author: William ZOUNON
# Description: script
#	- retrieves the zip
#	- dezzipe
#	- configures the environment
#	- creates dataproc cluster if none exists
#	- sends a pyspark job to the dataproc cluster
#	- deletes files and cluster
# ----------------------------------------------------------------------------


# Configuration
PROJECT_ID=${PROJECT_ID:-"analytics-trafic-idfm"}
REGION=${REGION:-"europe-west9"}
CLUSTER_NAME="cluster-spark-analytics"
SOURCE_BUCKET="gs://zip_data_gtfs_idfm"
DEST_BUCKET="gs://analytics_trafic_idfm/raw_data_gtfs_idfm"

# Function to send a notification
send_notification() {
    local message=$1
    gcloud pubsub topics publish notifications-dataproc --message="$message"
}

# Function to check if the cluster exists
check_cluster_exists() {
    gcloud dataproc clusters list \
        --region=$REGION \
        --format="value(clusterName)" \
        | grep -q "^$CLUSTER_NAME$"
    return $?
}

# Function to create a cluster
create_cluster() {
    echo "Dataproc cluster creation."
    gcloud dataproc clusters create $CLUSTER_NAME \
        --enable-component-gateway \
        --region $REGION \
        --master-machine-type e2-standard-2 \
        --master-boot-disk-type pd-balanced \
        --master-boot-disk-size 50 \
        --num-workers 2 \
        --worker-machine-type n2-standard-4 \
        --worker-boot-disk-type pd-balanced \
        --worker-boot-disk-size 200 \
        --image-version 2.1-debian11 \
        --optional-components JUPYTER,DOCKER \
        --initialization-actions 'gs://analytics_trafic_idfm/source_code/init_create_dataproc_cluster.sh' \
        --project $PROJECT_ID
}

# Function to delete the cluster
delete_cluster() {
    echo "Dataproc cluster deleting."
    gcloud dataproc clusters delete $CLUSTER_NAME \
        --region=$REGION \
        --quiet
}

# Function to manage decompression and file transfer
unzip_source_file() {
    local zip_file=$1
    local temp_dir=$(mktemp -d)
    
    # Copy zip in local
    gsutil cp "$SOURCE_BUCKET/$zip_file" "$temp_dir/"
    
    # Unziping
    cd "$temp_dir"
    unzip -o "$zip_file"

    if [ $? -eq 0 ]; then
        echo "Unzip success."
    else
        echo "Unzip failed."
        return 1
    fi
        
    # Copy file on cloud storage
    gsutil -m cp * "$DEST_BUCKET/"
    RS=$?
    
    # Cleaning
    cd - > /dev/null
    rm -rf "$temp_dir"

    return $RS
}

# Function to setup environment
setup_environment() {
    echo "Retrieving the git repository."
    rm -rfd analyse_trafic_idfm_gcp
    git clone https://github.com/Wizo17/analyse_trafic_idfm_gcp.git

    if [ $? -eq 0 ]; then
        cd analyse_trafic_idfm_gcp
        cp .env_prod .env

        if [ -f .env ]; then
            rm -f etl_source_code.zip
            cd src
            zip -r ../etl_source_code.zip *
            cd ..
            zip -g etl_source_code.zip .env
            zip -g etl_source_code.zip setup.py

            gsutil cp -r etl_source_code.zip gs://analytics_trafic_idfm/source_code/
            gsutil cp -r requirements.txt gs://analytics_trafic_idfm/source_code/
            gsutil cp -r .env gs://analytics_trafic_idfm/source_code/
            gsutil cp -r gcp/init_create_dataproc_cluster.sh gs://analytics_trafic_idfm/source_code/

            return $?
        else
            echo ".env file not found!"
            return 1
        fi
    else
        echo "An error occurred while cloning the repository."
        return 1
    fi
}

# main function
main() {
    local zip_file=$1

    # Check zip file
    if [[ ! $zip_file =~ \.zip$ ]]; then
        echo "It is not zip file"
        exit 1
    fi

    # Setup environnement
    gcloud config set project $PROJECT_ID
    gcloud config set dataproc/region $REGION
    gcloud config set compute/region $REGION
    if ! setup_environment; then
        send_notification "Somenthing wrong with environment, check git, .env, cloud storage."
        exit 1
    fi
    
    # Unzip
    if ! unzip_source_file "$zip_file"; then
        exit 1
    fi
    
    # Check/Create cluster
    if ! check_cluster_exists; then
        create_cluster
        if [ $? -ne 0 ]; then
            send_notification "Somenthing wrong with dataproc cluster creation."
            exit 1
        fi
    fi
    
    DATE_VAR=$(date +%F)

    # Run pyspark job
    echo "Submit PySpark job."
    JOB_ID=$(gcloud dataproc jobs submit pyspark \
        --cluster "$CLUSTER_NAME" \
        --region "$REGION" \
        --py-files "gs://analytics_trafic_idfm/source_code/etl_source_code.zip" \
        --files "gs://analytics_trafic_idfm/source_code/.env" \
        --format='value(reference.jobId)' \
        src/main.py -- "$DATE_VAR")
            
    gcloud dataproc jobs wait $JOB_ID --region=$REGION
    JOB_STATUS=$?
    
    if [ $JOB_STATUS -eq 0 ]; then
        echo "Job successfully completed"
        
        # Delete files
        echo "Deleting files."
        gsutil rm  -r "$SOURCE_BUCKET/$zip_file"
        # gsutil rm  -r "$DEST_BUCKET/*"
        
        # Cluster deletion
        # delete_cluster

        send_notification "PySpark job sucess"
    else
        echo "PySpark job failed"
        send_notification "PySpark job failed"
        exit 1
    fi
}

# Error handling
set -e
trap 'send_notification "Unexpected error in the script"' ERR

# Ru script
main "$1"

