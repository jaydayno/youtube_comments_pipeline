# Creating IAM role and policy, then attaching together for AWS Lambda
resource "aws_iam_role" "lambda_role" {
name   = "Lambda_Function_Role"
assume_role_policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": "sts:AssumeRole",
     "Principal": {
       "Service": "lambda.amazonaws.com"
     },
     "Effect": "Allow",
     "Sid": ""
   }
 ]
}
EOF
}

resource "aws_iam_policy" "iam_policy_for_lambda" {
 
name         = "aws_iam_policy_for_terraform_aws_lambda_role"
path         = "/"
description  = "AWS IAM Policy for managing aws lambda role"
policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObjectAcl",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::*/*"
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "attach_iam_policy_to_iam_role" {
role        = aws_iam_role.lambda_role.name
policy_arn  = aws_iam_policy.iam_policy_for_lambda.arn
}

# Zipping processing script for lambda
data "archive_file" "zip_the_python_code" {
type        = "zip"
source_file  = "${path.module}/../dags/scripts/processing_json.py"
output_path = "${path.module}/../dags/scripts/processing_json.zip"
}

# AWS Lambda
resource "aws_lambda_function" "terraform_lambda_func" {
filename                       = "${path.module}/../dags/scripts/processing_json.zip"
function_name                  = "Test_Lambda_Function"
role                           = aws_iam_role.lambda_role.arn
handler                        = "processing_json.lambda_handler"
runtime                        = "python3.8"
depends_on                     = [aws_iam_role_policy_attachment.attach_iam_policy_to_iam_role]
memory_size                    = 128
layers                         = ["${aws_lambda_layer_version.python38-deployment-package.arn}"]
}

resource "aws_lambda_layer_version" "python38-deployment-package" {
  filename            = "my-deployment-package.zip"
  layer_name          = "Python3-pandas"
  source_code_hash    = "${filebase64sha256("my-deployment-package.zip")}"
  compatible_runtimes = ["python3.8"]
}