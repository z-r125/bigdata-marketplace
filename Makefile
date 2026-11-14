.RECIPEPREFIX = >
SHELL := /bin/bash
SPARK_PKG := com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
JOB ?= 00_smoke

.PHONY: up status ps schema gen load bench spark kill3 heal3 dash logs down clean

up:
> docker compose up -d --build

status:
> docker exec cass1 nodetool status

ps:
> docker compose ps

schema:
> for f in cql/*.cql; do echo ">> $$f"; docker exec -i cass1 cqlsh < $$f || exit 1; done

gen:
> docker exec -u $$(id -u):$$(id -g) app python generator/generate.py --seed 42 --ref-date 2026-11-13
load:
> docker exec app python loader/load.py

bench:
> docker exec app python bench/run_all.py

spark:
> docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages $(SPARK_PKG) --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.cassandra.connection.host=cass1,cass2,cass3 /opt/app/jobs/$(JOB).py

kill3:
> docker stop cass3

heal3:
> docker start cass3

dash:
> docker exec -d app streamlit run dashboard/app.py --server.address 0.0.0.0

logs:
> docker compose logs -f $(S)

down:
> docker compose down

clean:
> docker compose down -v
tx:
> docker exec app python -m app.transactional.demo