
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('smallTest').getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","dataproc")

sql = """
SQL for BigQuery
"""
# e.g.
# sql = """
# SELECT
# count(*) as count
# FROM `{project_id}.bq_public_data.github_repos_commits`
# """
df = spark.read.format("bigquery").option('query', sql).load()
df.show()
