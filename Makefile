.RECIPEPREFIX = >
SHELL := /bin/bash
SPARK_PKG := com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
JOB ?= 00_smoke
DEFAULT_CL ?= LOCAL_QUORUM

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
> docker exec -u $$(id -u):$$(id -g) -e CASS_DEFAULT_CL=$(DEFAULT_CL) app python -m bench.run_all $(ARGS)

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

index-exp:
> rm -f bench/results/index.csv
> $(MAKE) bench ARGS="--only A21,A23,A24 --variant sai --runs 20 --out bench/results/index.csv"
> docker exec cass1 cqlsh -k marketplace -e "DROP INDEX IF EXISTS products_stock_sai; DROP INDEX IF EXISTS products_category_sai; DROP INDEX IF EXISTS products_ram_sai; DROP INDEX IF EXISTS products_price_sai; DROP INDEX IF EXISTS customers_last_order_sai;"
> sleep 10
> -docker exec cass1 cqlsh -k marketplace -e "SELECT name FROM products WHERE stock < 10 LIMIT 1;"
> $(MAKE) bench ARGS="--only A21_NOIDX,A23_NOIDX,A24_NOIDX --variant sans_index --runs 20 --out bench/results/index.csv"
> docker exec -i cass1 cqlsh < cql/03_indexes.cql
> sleep 30
> docker exec -u $$(id -u):$$(id -g) app python -m bench.report_index