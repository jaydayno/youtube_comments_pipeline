cd terraform/
terraform output > ../dags/scripts/configuration.env
pass_string=$(terraform output db_password)
echo "db_full_password = ${pass_string}" >> ../dags/scripts/configuration.env
cat $HOME/.aws/credentials >> ../dags/scripts/configuration.env
echo "TOKEN = BQC7LyynJ8td0R3NfmB9m7WZ4s3VdR20rjl857n7YZU2lh6CijQ-uwCdC23rwenorCTdoOs_3BjcsA9GtP1RF5BVdtm1CFKIqB9D86CrZ-fiyeULsTHn_lVFL6e5iVQc5IzoKwbmzZXiYokWS_L6Ycz5HQGolTGSrfhLfYsK0eJPccvhVU4" >> ../dags/scripts/configuration.env