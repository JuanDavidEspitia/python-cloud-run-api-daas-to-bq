from flask import Flask, request
import pandas as pd
from google.cloud import bigquery
import logging
import google.cloud.logging

app = Flask(__name__)
client = google.cloud.logging.Client()
client.setup_logging()


def get_info(numero_identificacion, tipo_identificacion, fecha_inicio, fecha_fin):
    """ Return a dataframe of query table PFM"""

    client = bigquery.Client()

    query_str = "SELECT attxn_tip_cta_afor, attxn_num_cta_afor, attxn_tip_transac, attxn_periodo, attxn_val_transac, attxn_desc_txn, tipo_trx, categoria, aaho_sal_hoy, acte_sal_hoy,\
            identificador, id_number_1, id_type_1 FROM `bdb-gcp-de-cds.ds_daas.export_pfm` WHERE  ID_Number_1 = @numero_identificacion  AND ID_Type_1 = @tipo_identificacion \
            AND identificador BETWEEN  @fecha_inicio AND @fecha_fin"

    logging.info("Query processed : {}".format(query_str))
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("numero_identificacion", "INT64", numero_identificacion),
            bigquery.ScalarQueryParameter("tipo_identificacion", "STRING", tipo_identificacion),
            bigquery.ScalarQueryParameter("fecha_inicio", "STRING", fecha_inicio),
            bigquery.ScalarQueryParameter("fecha_fin", "STRING", fecha_fin),
        ]
    )
    dataframe = (
        client.query(query_str, job_config=job_config)
        .result()
        .to_dataframe()
    )

    logging.info("Query completed {}".format(dataframe.size))
    return dataframe.to_json(orient="records")


@app.route("/", methods=["POST"])
def main_http():
    """ Return a response of data PFM format json """
    request_json = request.get_json(silent=True)

    if 'numero_identificacion' and 'tipo_identificacion' and 'fecha_inicio' and 'fecha_fin' in request_json:
        numero_identificacion = request_json['numero_identificacion']
        tipo_identificacion = request_json['tipo_identificacion']
        fecha_inicio = request_json['fecha_inicio']
        fecha_fin = request_json['fecha_fin']
        dataframe_file = get_info(numero_identificacion, tipo_identificacion, fecha_inicio, fecha_fin)

        response = dataframe_file

    else:
        logging.info("The info is not completed")
        response = {"key": "value"}

    return response


if __name__ == "__main__":
    # Used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    app.run(host="localhost", port=8080, debug=True)