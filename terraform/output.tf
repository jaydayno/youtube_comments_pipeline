output "aws_region" {
  description = "Region set for AWS"
  value       = var.aws_region
}

## S3
output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.spotify-bucket.id
}

## Lambda
output "lambda_function_name" {
  description = "lambda function name."
  value       = aws_lambda_function.terraform_lambda_func.function_name
}