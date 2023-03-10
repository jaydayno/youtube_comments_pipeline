variable "aws_region" {
  description = "Region for the AWS services to run in."
  type        = string
  default     = "us-east-1"
}

## S3
variable "bucket_prefix" {
  description = "Bucket prefix for the S3"
  type        = string
  default     = "youtube-pipeline-"
}

## Alert email receiver
variable "alert_email_id" {
  description = "Email id to send alerts to "
  type        = string
  default     = "jaydendayno@gmail.com"
}
