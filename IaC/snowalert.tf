resource "aws_s3_bucket" "sfc-snowalert-s3" {
  bucket = "sfc-snowalert-deploy"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

resource "aws_iam_role" "sfc-snowalert-role" {
  name        = "sfc-snowalert-role"
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

data "aws_iam_policy" "lambda-vpc-access" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda-vpc-policy" {
  role       = "${aws_iam_role.sfc-snowalert-role.name}"
  policy_arn = "${data.aws_iam_policy.lambda-vpc-access.arn}"
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
        "kms:Decrypt"
      ],
      "Resource": "${aws_kms_key.sfc-snowalert-key.arn}"
    }
  ]
}
POLICY
}

resource "aws_cloudwatch_event_rule" "sfc-snowalert-jamf" {
  name        = "sfc-snowalert-jamf-cloudwatch-rule"
  description = "rule to trigger the jamf snowalert query daily"

  schedule_expression = "cron(5 10 * * ? *)"
}

resource "aws_cloudwatch_event_target" "sfc-snowalert-jamf" {
  rule = "${aws_cloudwatch_event_rule.sfc-snowalert-jamf.name}"
  arn  = "${aws_lambda_function.sfc-snowalert-jamf.arn}"
}

resource "aws_lambda_function" "sfc-snowalert-jamf" {
  function_name = "sfc-snowalert-jamf"
  handler       = "jamf_alert.lambda_handler"
  role          = "${aws_iam_role.sfc-snowalert-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.sfc-snowalert-s3.id}"
  s3_key        = "jamf_alert.zip"
  timeout       = "120"

  vpc_config = {
    subnet_ids         = ["subnet-09e219cf8f803d97a"]
    security_group_ids = ["sg-0f65fa84d7bdc98bb"]
  }

  environment {
    variables = {
      auth = "AQICAHgeGObvsFH0axIIfBAFflcHJOlt9LDwyCYJs4w3Jr9VKgECTNEgwZ1y00u/GPsWaa2JAAAAgzCBgAYJKoZIhvcNAQcGoHMwcQIBADBsBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDHFdGEhzt92fguNLlwIBEIA/Hnb+jnpkz6ZSiI1zfG7pLgE0jQvZDcPTuMNv4X79uJcMIXAM3Q5wT+CnDO4NCSGeUTqX0heH6krmQqyF2o7m"
    }
  }
}

resource "aws_cloudwatch_event_rule" "jira" {
  name        = "sfc-jira-cloudwatch-rule"
  description = "rule to trigger the jira creation hourly"

  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "jira" {
  rule = "${aws_cloudwatch_event_rule.jira.name}"
  arn  = "${aws_lambda_function.sfc-snowalert-jira.arn}"
}

resource "aws_lambda_function" "sfc-snowalert-jira" {
  function_name = "sfc-snowalert-jira"
  handler       = "alert_handler.lambda_handler"
  role          = "${aws_iam_role.sfc-snowalert-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.sfc-snowalert-s3.id}"
  s3_key        = "alert_handler.zip"
  timeout       = "300"

  vpc_config = {
    subnet_ids         = ["subnet-09e219cf8f803d97a"]
    security_group_ids = ["sg-0f65fa84d7bdc98bb"]
  }

  environment {
    variables = {
      SNOWALERT_PASSWORD = "AQICAHgeGObvsFH0axIIfBAFflcHJOlt9LDwyCYJs4w3Jr9VKgECTNEgwZ1y00u/GPsWaa2JAAAAgzCBgAYJKoZIhvcNAQcGoHMwcQIBADBsBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDHFdGEhzt92fguNLlwIBEIA/Hnb+jnpkz6ZSiI1zfG7pLgE0jQvZDcPTuMNv4X79uJcMIXAM3Q5wT+CnDO4NCSGeUTqX0heH6krmQqyF2o7m"
      JIRA_API_USER      = "snowalert@snowflake.net"
      JIRA_API_PASSWORD  = "AQICAHgeGObvsFH0axIIfBAFflcHJOlt9LDwyCYJs4w3Jr9VKgFuSks/ctzocW/WO638WDTwAAAAfTB7BgkqhkiG9w0BBwagbjBsAgEAMGcGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMpFZWE9DXDmG7L6uzAgEQgDqz+nCLSg2c5fFpTrJs1fT4rWS/b7O3pJUuflGygUsQ8mhXVEk58NBA4YvVONQFoW9mzwt0Qo/Lt2Yz"
      SNOWALERT_ACCOUNT  = "oz03309"
      SNOWALERT_USER     = "snowalert"
    }
  }
}

resource "aws_kms_key" "sfc-snowalert-key" {
  description = "This key encrypts the credentials for the snowalert user in Snowflake"
}

resource "aws_kms_alias" "sfc-snowalert-key" {
  name          = "alias/sfc-snowalert-key"
  target_key_id = "${aws_kms_key.sfc-snowalert-key.key_id}"
}
