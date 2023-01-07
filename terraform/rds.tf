resource "aws_db_instance" "db-postgres" {
  identifier             = "db-postgres"
  instance_class         = "db.t3.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "14.1"

  username               = "dbadmin"
  password               = var.db_password
  db_name                = "spotify_song_db"
  
  publicly_accessible    = true
  skip_final_snapshot    = true
  vpc_security_group_ids = [aws_security_group.redshift_security_group.id]
}

# For feature_name --> run command 
# "aws rds describe-db-engine-versions --engine [ENGINE] --engine-version [ENGINE-VERSION]"
# and check for "SupportedFeatureNames"
resource "aws_db_instance_role_association" "example" {
  db_instance_identifier = aws_db_instance.db-postgres.id
  feature_name           = "s3Import" 
  role_arn               = aws_iam_role.lambda_role.arn
}


resource "aws_security_group" "redshift_security_group" {
  name = "redshift_security_group"
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}