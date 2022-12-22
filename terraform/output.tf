output "aws_region" {
  description = "Region set for AWS"
  value       = var.aws_region
}

## S3
output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.spotify-bucket.id
}