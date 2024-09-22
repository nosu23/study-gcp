import argparse

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def upload_to_gcs(project_id, bucket_name, model_file_name) -> None:
    """
    upload the files required for dataproc to work to the GCS
    :param project_id:
    :param bucket_name:
    :param model_file_name:
    :return:
    """
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)

    blob_gcs = bucket.blob(model_file_name)

    local_path = f'pyspark/{model_file_name}'
    blob_gcs.upload_from_filename(local_path)


def quickstart(project_id, region, cluster_name, job_file_path):
    """
    simple job to connect to BigQuery from pyspark
    :param project_id:
    :param region:
    :param cluster_name:
    :param job_file_path:
    :return:
    """
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": job_file_path},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    aa = 100  # for breakpoint


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', help='gcp project_id')
    args = parser.parse_args()

    project_id    = args.project_id
    region        = 'us-central1'
    cluster_name  = 'single-node-cluster'


    bucket_name      = f'{project_id}-pyspark-files'

    schema           = 'simple'
    #model_identifier = 'pyspark01'
    model_identifier = 'pyspark02'
    model_file_name = f"{schema}/{model_identifier}.py"

    gcs_location = f"gs://{bucket_name}/{model_file_name}"

    upload_to_gcs(project_id=project_id, bucket_name=bucket_name, model_file_name=model_file_name)

    quickstart(project_id, region, cluster_name, gcs_location)

