.PHONY: docker-config start stop clean stats connect-shell pg-shell be-shell localstack-shell jupyter-shell


docker-config:
	docker compose -f docker-compose.yml config

start:
	docker compose -f docker-compose.yml up --build

stop:
	docker compose -f docker-compose.yml down

clean:
	docker compose -f docker-compose.yml down -v

stats:
	docker compose stats

connect-shell:
	docker compose exec -it connect bash

pg-shell:
	docker compose exec -it postgres psql -U airflow -d airflow

be-shell:
	docker compose exec -it backend bash

localstack-shell:
	docker compose exec -it localstack-s3 bash

jupyter-shell:
	docker compose exec -it jupyter-lab bash