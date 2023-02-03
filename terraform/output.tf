output "aws_region" {
  description = "Region set for AWS"
  value       = var.aws_region
}

## S3
output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.youtube-bucket.id
}

## Lambda
output "lambda_function_name" {
  description = "lambda function name."
  value       = aws_lambda_function.terraform_lambda_func.function_name
}

## rds instance (postgres)
output "db_id" {
  description = "rds id."
  value       = aws_db_instance.db-postgres.id
}

output "db_main_name" {
  description = "Name of the database (postgres) on rds."
  value       = aws_db_instance.db-postgres.db_name
}

output "db_host" {
  description = "Host name of database (postgres) on rds."
  value       = aws_db_instance.db-postgres.address
}

output "db_port" {
  description = "Port of database (postgres) on rds."
  value       = aws_db_instance.db-postgres.port
}

output "db_username" {
  value     = aws_db_instance.db-postgres.username
}