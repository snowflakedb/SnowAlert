variable "password" {}
variable "jira_password" {}
variable jira_user {}
variable jira_project {}
variable jira_url {}
variable snowflake_account {}
variable jira_flag {}
variable s3_bucket_name {}
variable query_runner_name {}
variable query_wrapper_name {}
variable suppression_runner_name {}
variable suppression_wrapper_name {}
variable jira_integration_name {}

resource "aws_s3_bucket" "snowalert-deploy" {
    bucket = "${var.s3_bucket_name}"

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
    versioning {
        enabled = true
    }
}

resource "aws_s3_bucket_object" "snowalert-query-wrapper" {
  bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  key = "query-wrapper.zip"
  source = "query_wrapper.zip"
}

resource "aws_s3_bucket_object" "snowalert-query-runner" {
  bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  key = "query_runner.zip"
  source = "query_runner.zip"
}

resource "aws_s3_bucket_object" "snowalert-suppression-wrapper" {
  bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  key = "suppression_wrapper.zip"
  source = "suppression_wrapper.zip"
}

resource "aws_s3_bucket_object" "snowalert-suppression-runner" {
  bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  key = "suppression_runner.zip"
  source = "suppression_runner.zip"
}

resource "aws_s3_bucket_object" "snowalert-jira-integration" {
  bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  key = "alert_handler.zip"
  source = "alert_handler.zip"
}

resource "aws_kms_key" "snowalert-key" {
    description = "key used to encrypt password for private key from snowalert"

     provisioner "local-exec" {
         command = "/bin/bash ./kms-helper.sh ${aws_kms_key.snowalert-key.key_id} ${var.password} ${var.jira_password}"
     }
}

data "local_file" "encrypted_password" {
    filename = "encrypted_password"

    depends_on = ["aws_kms_key.snowalert-key"]
}

data "local_file" "encrypted_private_key" {
    filename = "rsa_key.b64"

    depends_on = ["aws_kms_key.snowalert-key"]
}

data "local_file" "encrypted_jira_password" {
  count = "${var.jira_flag}"
  filename = "encrypted_jira_password"

  depends_on = ["aws_kms_key.snowalert-key"]
}

resource "aws_iam_role" "snowalert-lambda-role" {
  name        = "snowalert-lambda-role"
  description = "Role for the Snowalert lambda functions"

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

resource "aws_iam_role_policy" "snowalert-lambda-role-policy" {
  name = "snowalert-lambda-role-policy"
  role = "${aws_iam_role.snowalert-lambda-role.name}"

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt"
      ],
      "Resource": "${aws_kms_key.snowalert-key.arn}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "${aws_lambda_function.snowalert-query-runner.arn}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "${aws_lambda_function.snowalert-suppression-runner.arn}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
POLICY
}

# To avoid uneccessary expenses, the cloudwatch events to schedule the lambdas are commented out
# If you are ready to schedule SnowAlert to check incoming data logs for events to alert on,
# uncomment the cloudwatch event rules and cloudwatch event targets in this file.

# resource "aws_cloudwatch_event_rule" "query-wrapper-event-rule" {
#   name        = "snowalert-query-wrapper-rule"
#   description = "Rule to trigger query wrapper every hour on the hour"

#   schedule_expression = "cron(0 */1 * * ? *)"
# }

# resource "aws_cloudwatch_event_target" "query-wrapper-target" {
#   rule = "${aws_cloudwatch_event_rule.query-wrapper-event-rule.name}"
#   arn  = "${aws_lambda_function.snowalert-query-wrapper.arn}"
# }

# resource "aws_cloudwatch_event_rule" "suppression-wrapper-event-rule" {
#   name        = "snowalert-suppression-wrapper-rule"
#   description = "Rule to trigger suppression wrapper every hour fifteen minutes past"

#   schedule_expression = "cron(15 */1 * * ? *)"
# }

# resource "aws_cloudwatch_event_target" "suppression-wrapper-target" {
#   rule = "${aws_cloudwatch_event_rule.suppression-wrapper-event-rule.name}"
#   arn  = "${aws_lambda_function.snowalert-suppression-wrapper.arn}"
# }

resource "aws_lambda_function" "snowalert-query-wrapper" {
  function_name = "${var.query_wrapper_name}"
  handler       = "query_wrapper.lambda_handler"
  role          = "${aws_iam_role.snowalert-lambda-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.snowalert-deploy.id}"
  s3_key        = "${aws_s3_bucket_object.snowalert-query-wrapper.id}"
  timeout       = "120"

  environment {
    variables = {
      private_key_password = "${data.local_file.encrypted_password.content}"
      private_key = "${data.local_file.encrypted_private_key.content}"
      account = "${var.snowflake_account}"
      SNOWALERT_QUERY_EXECUTOR_FUNCTION = "snowalert-query-runner"
    }
  }
  depends_on = ["aws_s3_bucket.snowalert-deploy"]
}

resource "aws_lambda_function" "snowalert-query-runner" {
  function_name = "${var.query_runner_name}"
  handler       = "query_runner.lambda_handler"
  role          = "${aws_iam_role.snowalert-lambda-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.snowalert-deploy.id}"
  s3_key        = "${aws_s3_bucket_object.snowalert-query-runner.id}"
  timeout       = "300"
  memory_size   = "512"


  environment {
    variables = {
      private_key_password = "${data.local_file.encrypted_password.content}"
      private_key = "${data.local_file.encrypted_private_key.content}"
      account = "${var.snowflake_account}"
    }
  }

  depends_on = ["aws_s3_bucket.snowalert-deploy"]
}

resource "aws_lambda_function" "snowalert-suppression-wrapper" {
  function_name = "${var.suppression_wrapper_name}"
  handler       = "suppression_wrapper.lambda_handler"
  role          = "${aws_iam_role.snowalert-lambda-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.snowalert-deploy.id}"
  s3_key        = "${aws_s3_bucket_object.snowalert-suppression-wrapper.id}"
  timeout       = "300"

  environment {
    variables = {
      private_key_password = "${data.local_file.encrypted_password.content}"
      private_key = "${data.local_file.encrypted_private_key.content}"
      account = "${var.snowflake_account}",
      SNOWALERT_SUPPRESSION_EXECUTOR_FUNCTION = "snowalert-suppression-runner"
    }
  }

  depends_on = ["aws_s3_bucket.snowalert-deploy"]
}

resource "aws_lambda_function" "snowalert-suppression-runner" {
  function_name = "${var.suppression_runner_name}"
  handler       = "suppression_runner.lambda_handler"
  role          = "${aws_iam_role.snowalert-lambda-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.snowalert-deploy.id}"
  s3_key        = "${aws_s3_bucket_object.snowalert-suppression-runner.id}"
  timeout       = "120"

  environment {
    variables = {
      private_key_password = "${data.local_file.encrypted_password.content}"
      private_key = "${data.local_file.encrypted_private_key.content}"
      account = "${var.snowflake_account}"
    }
  }

  depends_on = ["aws_s3_bucket.snowalert-deploy"]
}

resource "aws_lambda_function" "snowalert-jira-integration" {
  count = "${var.jira_flag}"
  function_name = "${var.jira_integration_name}"
  handler = "alert_handler.lambda_handler"
  role = "${aws_iam_role.snowalert-lambda-role.arn}"
  runtime = "python3.6"
  s3_bucket = "${aws_s3_bucket.snowalert-deploy.id}"
  s3_key = "${aws_s3_bucket_object.snowalert-jira-integration.id}"
  timeout = "300"

  environment {
    variables = {
      JIRA_API_USER = "${var.jira_user}"
      SNOWALERT_JIRA_PROJECT = "${var.jira_project}"
      SNOWALERT_ACCOUNT = "${var.snowflake_account}"
      SNOWALERT_JIRA_URL = "${var.jira_url}"
      JIRA_API_PASSWORD = "${data.local_file.encrypted_jira_password.content}"
      private_key_password = "${data.local_file.encrypted_password.content}"
      private_key = "${data.local_file.encrypted_private_key.content}"
      SNOWALERT_USER = "snowalert"
      PROD_FLAG = "True"
    }
  }
}

# resource "aws_cloudwatch_event_rule" "snowalert-jira-integration-rule" {
#   count = "${var.jira_flag}"
#   name = "snowalert-jira-cloudwatch"
#   description = "rule to trigger the creation of Jira tickets every fifteen minutes"
  
#   schedule_expression = "cron(*/15 * * * ? *)"
# }

# resource "aws_cloudwatch_event_target" "snowalert-jira-integration-target" {
#   rule = "${aws_cloudwatch_event_rule.snowalert-jira-integration-rule.name}"
#   arn = "${aws_lambda_function.snowalert-jira-integration.arn}"
# }
