airflow-up:
	docker-compose up airflow-init && docker-compose up -d

zip:
	chmod +x generate_zip.sh
	./generate_zip.sh

infra:
	terraform -chdir=terraform/ init -input=false
	terraform -chdir=terraform/ apply

config:
	chmod +x generate_config.sh
	./generate_config.sh

infra-down:
	terraform -chdir=terraform/ destroy

airflow-down:
	docker-compose down --volumes --rmi all
