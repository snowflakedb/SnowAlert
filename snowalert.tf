resource "aws_s3_bucket" "sfc-snowalert-s3" {
    bucket = "sfc-snowalert-alerts"
    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

resource "aws_iam_role" "sfc-snowalert-role" {
    name = "sfc-snowalert-role"
    description = "Role for the Snowalert lambda function"
    assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "sfc-snowalert-role-policy" {
    name = "sfc-snowalert-role-policy"
    role = "${aws_iam_role.sfc-snowalert-role.name}"
    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "${aws_s3_bucket.sfc-snowalert-s3.arn}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt"
      ],
      "Resource": "${aws_kms_key.sfc-snowalert-key.arn}"
    }
  ]
}
POLICY
}

resource "aws_lambda_function" "sfc-snowalert-lambda" {
    function_name = "sfc-snowalert-lambda"
    handler = "sfc-snowalert-lambda.lambda_handler"
    role = "${aws_iam_role.sfc-snowalert-role.arn}"
    runtime = "python3.6"
    filename = "sfc-snowalert-lambda.zip"
    timeout = "120"

    environment {
        variables = {
            auth = "AQICAHgeGObvsFH0axIIfBAFflcHJOlt9LDwyCYJs4w3Jr9VKgECTNEgwZ1y00u/GPsWaa2JAAAAgzCBgAYJKoZIhvcNAQcGoHMwcQIBADBsBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDHFdGEhzt92fguNLlwIBEIA/Hnb+jnpkz6ZSiI1zfG7pLgE0jQvZDcPTuMNv4X79uJcMIXAM3Q5wT+CnDO4NCSGeUTqX0heH6krmQqyF2o7m"
            }
        }
}

resource "aws_kms_key" "sfc-snowalert-key" {
    description = "This key encrypts the credentials for the snowalert user in Snowflake"
}
