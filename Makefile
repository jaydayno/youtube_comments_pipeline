####################################################################################################################
# Setup containers to run Airflow.

airflow-up:
	docker-compose up airflow-init && docker-compose up -d

infra:
	terraform -chdir=terraform/ init -input=false
	terraform -chdir=terraform/ apply

infra-down:
	terraform -chdir=terraform/ destroy

airflow-down:
	docker-compose down --volumes --rmi all
