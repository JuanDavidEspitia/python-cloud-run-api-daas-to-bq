# python-cloud-run-api-daas-to-bq
## Projecto Python de Backend de API en Cloud Run (GCP)

Projecto Python que se encarga de recibir un Request desde AWS y se recibe atraves de un Endpoint que expone Cloud Run.

Cloud Run funcionaria como backend para recibir la peticion y de los parametros se los envia a BQ para que sea BQ quien se encargue de procesar los datos.

Luego envia el responde encriptado con RSA y llaves asimetricas para el descirfrado de los datos.

