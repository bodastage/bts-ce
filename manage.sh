#!/bin/bash
#
#
docker run -p 3141:8080 --name docker-bts-mediation -v $(pwd)/mediation:/mediation -v $(pwd)/mediation/dags:/usr/local/airflow/dags -v $(pwd)/mediation/logs:/usr/local/airflow/logs --net=host --env POSTGRES_HOST=$(docker-machine ip) -d bts-mediation