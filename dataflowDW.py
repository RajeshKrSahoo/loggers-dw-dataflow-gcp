
# Configuration
# project_id = "gcp-sa-431119"
# subscription_id  = "server-log-sub-raj"
# dataset_id = "log_dataset_raj"
# TABLE_ID = "processed_logs_raj"
# FULL_TABLE_PATH = f"{project_id}:{dataset_id}.{TABLE_ID}"

# print("project")

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
from google.cloud import pubsub_v1


# Set up your GCP project, subscription, and BigQuery table
project_id = "gcp-sa-431119"
subscription_id = "server-log-sub-raj"
bigquery_table = "log_dataset_raj.processed_logs_raj"


# Function to parse the incoming Pub/Sub messages
def parse_message(message):
   data = json.loads(message)
   return {
       "timestamp": data["timestamp"],
       "log_level": data["log_level"],
       "service": data["service"],
       "message": data["message"]
   }


# Custom class to write the transformed data to BigQuery
class WriteToBigQuery(beam.DoFn):
    def __init__(self, table):
       self.table = table


    def process(self, element):
       # Convert the element to a format that BigQuery understands and yield it
       yield beam.io.gcp.bigquery.TableRowJsonCoder().encode(element)


    # Set up the pipeline options
    options = PipelineOptions([
        '--project=gcp-sa-431119',             # Replace with your project ID
        '--region=us-central1',                # Replace with your region
        '--runner=DataflowRunner',             # Ensure the pipeline runs on Dataflow
        '--job_name=log-processing-job-rajsahoo',       # Set a unique job name
        '--streaming',                         # This flag enables streaming mode
    ])

    # Using the pipeline options with Apache Beam
    with beam.Pipeline(options=options) as pipeline:
        (
        pipeline
        # Read from Pub/Sub
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{subscription_id}")
        # Decode the message from bytes to string
        | "Decode Message" >> beam.Map(lambda x: x.decode("utf-8"))
        # Parse the JSON data
        | "Parse JSON" >> beam.Map(parse_message)
        # Write the processed data to BigQuery
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=bigquery_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Ensure data is appended
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Create table if it doesn't exist
        )
    )



