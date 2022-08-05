import pandas as pd
import numpy as np
import csv
from google.cloud import bigquery
import google.cloud.logging
import logging
import json
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Random import get_random_bytes
from Crypto.PublicKey import RSA
from base64 import b64decode, b64enco
from decimal import Decimal
AES_KEY_SIZE = 16
BLOCK_SIZE = 16


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


client = google.cloud.logging.Client.from_service_account_json(
    r'C:\Users\Admin\Documents\Proyectos BancoDeBogota\credentials_google2.json')
client.setup_logging()


def get_info(numero_identificacion, tipo_identificacion, fecha_inicio, fecha_fin):
    """ Return a dataframe of query table PFM"""

    client = bigquery.Client.from_service_account_json(
        r'C:\Users\Admin\Documents\Proyectos BancoDeBogota\credentials_google2.json')

    query_str = "SELECT  categorias, identificador,tipo_producto,num_producto, id_number_1, id_type_1\
             FROM `proven-airship-330215.test_dlp.test_user` WHERE  ID_Number_1 = @numero_identificacion  AND ID_Type_1 = @tipo_identificacion \
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

    # dataframe['identificador']= pd.to_datetime(dataframe['identificador'])
    # dataframe["identificador"]= dataframe.identificador.values.astype(np.int64) // 10 ** 9
    # json_list = json.loads(json.dumps(list(dataframe.T.to_dict().values()), cls=DecimalEncoder))
    res = dataframe.to_json(orient="records")
    res2 = json.loads(res)
    return res, res2, dataframe  # dataframe, json_list#.to_json(orient="records")

# Enviamos los parametros para la pueba
res, res2, df = get_info(67024277, "CC", "2021-10-01", "2021-11-25")


def encrypt(cipher_rsa, data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    #Generate a session key randomly
    sessionkey = get_random_bytes(AES_KEY_SIZE)
    #Encrypt session key with RSA public key
    encsessionkey = cipher_rsa.encrypt(sessionkey)
    cipher_aes = AES.new(sessionkey, AES.MODE_EAX)
    #AES EAX mode is used to encrypt and generate verification tag
    ciphertext, tag = cipher_aes.encrypt_and_digest(data)
    #Assemble ciphertext session key + nonce+tag + data ciphertext, where nonce can be understood as IV
    return b64encode(encsessionkey + cipher_aes.nonce + tag + ciphertext)

def decrypt(rsakey,cipher_rsa, data):
    if isinstance(data, str):
        data = bytes.fromhex(data)
    pos = 0
    encsessionkey = data[pos:rsakey.size_in_bytes()]
    pos += rsakey.size_in_bytes()
    nonce = data[pos:pos + AES_KEY_SIZE]
    pos += AES_KEY_SIZE
    tag = data[pos:pos + 16]
    pos += 16
    ciphertext = data[pos:]
    sessionkey = cipher_rsa.decrypt(encsessionkey)
    cipher_aes = AES.new(sessionkey, AES.MODE_EAX, nonce)
    plaintext = cipher_aes.decrypt_and_verify(ciphertext, tag)
    plaintext = plaintext.decode('utf-8')
    return plaintext


with open("public.pem", "rb") as key1:
    keyPublic = RSA.importKey(key1.read())
with open("private.pem", "rb") as key2:
    keyPrivate = RSA.importKey(key2.read())

cipher_rsa = PKCS1_OAEP.new(keyPublic)
cipher_rsa2 = PKCS1_OAEP.new(keyPrivate)


# Encriptamos el DF de retorno
encryp = encrypt(cipher_rsa, res)


with open(".\keys\private.pem", "rb") as key2:
    keyPrivate = RSA.importKey(key2.read())

#cipher_rsa = PKCS1_OAEP.new(keyPublic)
cipher_rsa2 = PKCS1_OAEP.new(keyPrivate)

output= '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'

texto =decrypt(keyPrivate,cipher_rsa2, b64decode(output))
print(texto)



res = json.loads(texto)
print(res)
res[0]
res[0]['categorias']
res2 = json.loads(res[0]['categorias'])








