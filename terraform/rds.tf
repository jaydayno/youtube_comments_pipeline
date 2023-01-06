resource "aws_db_instance" "db-postgres" {
  identifier             = "db-postgres"
  instance_class         = "db.t3.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "14.1"
  username               = "dbadmin"
  password               = var.db_password
  publicly_accessible    = true
  skip_final_snapshot    = true
}

# run command "aws rds describe-db-engine-versions --engine [ENGINE] --engine-version [ENGINE-VERSION]"
# and check for "SupportedFeatureNames"
resource "aws_db_instance_role_association" "example" {
  db_instance_identifier = aws_db_instance.db-postgres.id
  feature_name           = "s3Import" 
  role_arn               = aws_iam_role.lambda_role.arn
}
