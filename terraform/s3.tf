# Create S3 bucket with a specific prefix
resource "aws_s3_bucket" "spotify-bucket" {
  bucket_prefix = var.bucket_prefix
  force_destroy = true
}