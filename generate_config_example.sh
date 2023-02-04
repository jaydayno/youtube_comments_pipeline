#!/bin/bash
cd terraform/
terraform output > ../dags/scripts/configuration.env
cat $HOME/.aws/credentials >> ../dags/scripts/configuration.env
pass_string=$(terraform output db_password)
echo "db_full_password = ${pass_string}" >> ../dags/scripts/configuration.env
yt_string=$(gcloud alpha services api-keys get-key-string __________________) #Paste your UID of Youtube API to get key's secret
declare -a array_split
array_split=(${yt_string})
echo "youtube_api_key = ${array_split[1]}" >> ../dags/scripts/configuration.env