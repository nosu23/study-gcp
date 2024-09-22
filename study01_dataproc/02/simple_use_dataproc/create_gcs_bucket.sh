GCS_FOR_PYSPARK_FILES='pyspark-files'
gcloud storage buckets create gs://${GCP_PROJECT_ID}-${GCS_FOR_PYSPARK_FILES}/ --uniform-bucket-level-access

GCS_FOR_DATAPROC="dataproc-gcs-bucket"
gcloud storage buckets create gs://${GCP_PROJECT_ID}-${GCS_FOR_DATAPROC}/ --uniform-bucket-level-access