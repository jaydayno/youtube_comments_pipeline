cd terraform/
terraform output > ../dags/scripts/configuration.env
pass_string=$(terraform output db_password)
echo "db_full_password = ${pass_string}" >> ../dags/scripts/configuration.env
cat $HOME/.aws/credentials >> ../dags/scripts/configuration.env