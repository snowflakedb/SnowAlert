resource "aws_s3_bucket" "jamf" { 
    bucket = "sfc-sec-jamf-logs"
    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

resource "aws_s3_bucket_notification" "sfc-jamf-bucket-notification" {
    bucket = "${aws_s3_bucket.jamf.id}"

    queue {
        queue_arn = "arn:aws:sqs:us-west-2:494544507972:sf-snowpipe-AIDAJTEARQU2ZK36WBN64-YvEUohqSRWDSojAz4MMXAg"
        events = ["s3:ObjectCreated:*"]
        filter_suffix = ".json"
    }
}

resource "aws_iam_role" "sfc-jamf-role" {
    name = "sfc-jamf-role"
    description = "Role for the jamf lambda function"
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

resource "aws_iam_role_policy" "sf-stage-sfc-sec-jamf-logs" {
    name = "sf-stage-sfc-sec-jamf-logs"
    role = "${aws_iam_role.sf-stage-sfc-sec-jamf-logs.name}"
    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": [
        "${aws_s3_bucket.jamf.arn}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetAccelerateConfiguration"
      ],
      "Resource": "${aws_s3_bucket.jamf.arn}"
    }
  ]
}
POLICY
}

resource "aws_iam_role" "sf-stage-sfc-sec-jamf-logs" {
    name = "sf-stage-sfc-sec-jamf-logs"
    description = "Role for Snowflake to access the s3 bucket"
    assume_role_policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
      {
          "Effect": "Allow",
          "Principal": {
              "AWS": [
                  "arn:aws:iam::494544507972:user/mtm2-s-ssca0695"
              ]
          },
          "Action": "sts:AssumeRole",
          "Condition": {
              "StringEquals": {
                  "sts:ExternalId": "OZ03309_SFCRole=4_/RzZzQpi4aYVMhr+73JzgiUYRmg="
              }
          }
      }
    ]
}
POLICY
}

resource "aws_iam_role_policy" "jamf" {
    name = "sfc-jamf-role"
    role = "${aws_iam_role.sfc-jamf-role.name}"

    policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Effect": "Allow",
        "Action": [
            "s3:PutObject"
            ],
        "Resource": "${aws_s3_bucket.jamf.arn}/*"
    },
    {
        "Effect": "Allow",
        "Action": [
            "kms:Decrypt"
        ],
        "Resource": "${aws_kms_key.jamf-key.arn}"
    }
  ]
}
POLICY
}

resource "aws_iam_policy" "snowpipe_jamf" {
    name = "snowpipe_jamf"
    description = "Policy for Snowpipe with Jamf"
    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "${aws_s3_bucket.jamf.arn}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetAccellerateConfiguration"
      ],
      "Resource": "${aws_s3_bucket.jamf.arn}"
    }
  ]
}
POLICY
}

resource "aws_kms_key" "jamf-key" {
    description = "This key encrypts the auth header for sfc-jamf lambda"
}

resource "aws_cloudwatch_event_rule" "jamf" {
    name = "sfc-jamf-cloudwatch-rule"
    description = "rule to trigger the jamf check daily"

    schedule_expression = "cron(0 10 * * ? *)"
}

resource "aws_cloudwatch_event_target" "jamf" {
    rule = "${aws_cloudwatch_event_rule.jamf.name}"
    arn = "${aws_lambda_function.jamf-lambda.arn}"
}

resource "aws_lambda_function" "jamf-lambda" {
    function_name = "sfc-jamf"
    handler = "sfc-jamf.lambda_handler"
    role = "${aws_iam_role.sfc-jamf-role.arn}"
    runtime = "python3.6"
    filename = "sfc-jamf.zip"
    timeout = "300"

    environment {
        variables = {
        auth = "AQICAHjyiS/Viv2yzlmrktqqbT5YyYFQRzG6QsdTrzP9wrHR1wECDP0uvaexXOi9HJWFfltqAAAAjTCBigYJKoZIhvcNAQcGoH0wewIBADB2BgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDJyi1ZIq5VMGsHVsCQIBEIBJWeKrEx33i1KHpVMQR+6I2CTdf1f/ScAHgpBw3QUoimrsplaXP1xVTrd5O+pzefTwA6eblpsuJFByY+kLFJDJl3a4rB1MSwtOIg=="
        }
    }
}
