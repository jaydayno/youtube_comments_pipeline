cd terraform/
terraform output > ../airflow/scripts/configuration.env
cat $HOME/.aws/credentials >> ../airflow/scripts/configuration.env