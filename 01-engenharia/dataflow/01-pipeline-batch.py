import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# EXERCÍCIO 1: Pipeline Batch com Apache Beam + Dataflow
# Objetivo: ler um CSV do GCS, transformar e gravar no BigQuery
#
# Pré-requisitos:
#   pip install apache-beam[gcp]
#   Suba um CSV de exemplo para: gs://SEU_BUCKET/input/taxi_sample.csv
#
# Para rodar localmente (DirectRunner):
#   python 01-pipeline-batch.py --runner=DirectRunner
#
# Para rodar no Dataflow (GCP):
#   python 01-pipeline-batch.py --runner=DataflowRunner


PROJECT   = "SEU_PROJETO_GCP"
BUCKET    = "SEU_BUCKET"
REGION    = "us-central1"
DATASET   = "engenharia_bigquery"
TABLE     = "taxi_trips_beam"
INPUT_CSV = f"gs://{BUCKET}/input/taxi_sample.csv"


def parse_csv(line):
    fields = line.split(",")
    return {
        "trip_id":              fields[0],
        "trip_start_timestamp": fields[1],
        "pickup_community_area": fields[2],
        "fare":                 float(fields[3]) if fields[3] else 0.0,
        "payment_type":         fields[4],
    }


def filtrar_corridas_validas(record):
    return record["fare"] > 0


SCHEMA = {
    "fields": [
        {"name": "trip_id",               "type": "STRING"},
        {"name": "trip_start_timestamp",  "type": "STRING"},
        {"name": "pickup_community_area", "type": "STRING"},
        {"name": "fare",                  "type": "FLOAT"},
        {"name": "payment_type",          "type": "STRING"},
    ]
}


def run():
    options = PipelineOptions()

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project        = PROJECT
    gcp_options.region         = REGION
    gcp_options.staging_location = f"gs://{BUCKET}/staging"
    gcp_options.temp_location    = f"gs://{BUCKET}/temp"
    gcp_options.job_name         = "pipeline-batch-taxi"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Ler CSV"          >> beam.io.ReadFromText(INPUT_CSV, skip_header_lines=1)
            | "Parsear linha"    >> beam.Map(parse_csv)
            | "Filtrar válidos"  >> beam.Filter(filtrar_corridas_validas)
            | "Gravar BigQuery"  >> WriteToBigQuery(
                table=f"{PROJECT}:{DATASET}.{TABLE}",
                schema=SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
