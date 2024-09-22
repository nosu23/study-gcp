#GCP_PROJECT_ID="nosu-test"

gcloud dataproc clusters create "single-node-cluster" \
    --region="us-central1" \
    --single-node \
    --project="${GCP_PROJECT_ID}"

