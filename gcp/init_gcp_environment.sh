#!/bin/bash

PROJECT_ID=${PROJECT_ID:-"analytics-trafic-idfm"}
MAINLAND=eu
REGION=europe-west9
REPOSITORY_NAME=analytics-repository
SERVICE_NAME=cloud-run-service
PROJECT_NUMBER=221748092362


gcloud config set project $PROJECT_ID
gcloud config set dataproc/region $REGION
gcloud config set compute/region $REGION

############################################ CLoud Storage Bucket ############################################
gsutil mb gs://zip_data_gtfs_idfm/
gsutil mb gs://analytics_trafic_idfm/
gsutil mb gs://analytics_trafic_idfm/raw_data_gtfs_idfm
gsutil mb gs://analytics_trafic_idfm/logs_etl_idfm/
gsutil mb gs://analytics_trafic_idfm/source_code/



############################################ BigQuery ############################################
bq mk --location=EU "analytics_dwh"



############################################ BigQuery ############################################
mkdir cloud-run-gcp
cd cloud-run-gcp

# TODO Copy cloud_run_functions_etl.py in folder 
# TODO Copy cloud_run_functions_etl.sh in folder 
# TODO Copy Dockerfile in folder 
# TODO Copy requirements_cloud_run.txt in folder 

#------------------- TEST -------------------#
# TODO Upload a zip file in gs://zip_data_gtfs_idfm and replace NAME.zip
chmod +x cloud_run_functions_etl.sh
./cloud_run_functions_etl.sh "NAME.zip"
#------------------- TEST -------------------#



############################################ Artifactory Registry ############################################
# TODO Update email address
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:USERNAME@example.com" \
  --role="roles/cloudbuild.builds.editor"


gcloud services enable artifactregistry.googleapis.com
gcloud artifacts repositories create $REPOSITORY_NAME \
    --repository-format=docker \
    --location=$REGION \
    --description="Repository for docker images"
gcloud artifacts repositories list --location=$REGION


#------------------- TEST -------------------#
docker build -t cloud-run-test .
docker run -p 8080:8080 -e PROJECT_ID=$PROJECT_ID -e REGION=$REGION cloud-run-test
# TODO Replace IDFM-gtfs_20250221_17.zip by your zip name and upload zip file in gs://zip_data_gtfs_idfm
nano test-event.json
# {
#   "name": "IDFM-gtfs_20250221_17.zip",
#   "bucket": "zip_data_gtfs_idfm",
#   "contentType": "application/zip",
#   "metageneration": "1",
#   "timeCreated": "2025-02-24T13:00:00.000Z",
#   "updated": "2025-02-24T13:00:00.000Z",
#   "size": "12345"
# }
# Other terminal in cloud shell
curl -X POST -H "Content-Type: application/json" -d @test-event.json http://localhost:8080/
#------------------- TEST -------------------#



############################################ Build Docker Image ############################################
gcloud auth configure-docker $REGION-docker.pkg.dev
# docker build -t gcr.io/$PROJECT_ID/cloud-run-gcp .
# docker tag gcr.io/$PROJECT_ID/cloud-run-gcp $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/cloud-run-gcp
docker build -t $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/cloud-run-gcp .
docker push $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/cloud-run-gcp
gcloud artifacts docker images list $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME



############################################ Pub/Sub topic ############################################
gcloud pubsub topics create notifications-dataproc



############################################ Cloud Run ############################################
# gcloud run services delete $SERVICE_NAME --region $REGION
gcloud run deploy $SERVICE_NAME \
    --image=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/cloud-run-gcp \
    --platform=managed \
    --region=$REGION \
    --allow-unauthenticated \
    --set-env-vars PROJECT_ID=$PROJECT_ID,REGION=$REGION \
    --memory=4Gi \
    --cpu=1 \
    --timeout=300


gcloud eventarc triggers create trigger-run-service \
  --location=$MAINLAND \
  --service-account=$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
  --destination-run-service=cloud-run-service \
  --destination-run-region=$REGION \
  --destination-run-path="/" \
  --event-filters="bucket=zip_data_gtfs_idfm" \
  --event-filters="type=google.cloud.storage.object.v1.finalized"



gcloud projects add-iam-policy-binding analytics-trafic-idfm \
  --member="serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding analytics-trafic-idfm \
  --member="serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.admin"


#------------------- TEST -------------------#
gcloud run services describe $SERVICE_NAME --region=$REGION

# curl -X POST -H "Content-Type: application/json" -d @test-event.json https://cloud-run-service-221748092362.europe-west9.run.app


gcloud run services logs read $SERVICE_NAME --region=$REGION
#------------------- TEST -------------------#






