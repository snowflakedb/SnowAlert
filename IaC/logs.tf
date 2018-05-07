resource "aws_s3_bucket" "logs" {
  bucket = "sfc-sec-logs"

  # Allow CloudTrail
  acl = "log-delivery-write"

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket = "${aws_s3_bucket.logs.id}"

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyUnEncryptedObjectUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "${aws_s3_bucket.logs.arn}/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
      }
    },
    {
      "Sid": " DenyUnEncryptedInflightOperations",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": "${aws_s3_bucket.logs.arn}/*",
      "Condition": {
        "Bool": {
          "aws:SecureTransport": "false"
        }
      }
    }
  ]
}
POLICY
}

resource "aws_iam_user" "snowflake_security_stage_user" {
  name = "snowflake_security_stage_user"
}

resource "aws_iam_user_policy" "snowflake_security_stage_user_read" {
  name = "snowflake_security_stage_user_read"
  user = "${aws_iam_user.snowflake_security_stage_user.name}"

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
            "Resource": "${aws_s3_bucket.logs.arn}/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "${aws_s3_bucket.logs.arn}"
        }
    ]
}
POLICY
}

resource "aws_iam_user" "logs_sfc-dev" {
  name = "logs_sfc-dev"
}

resource "aws_iam_user_policy" "logs_sfc-dev" {
  name = "logs_sfc-dev"
  user = "${aws_iam_user.logs_sfc-dev.name}"

  policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": "${aws_s3_bucket.logs.arn}/aws/sfc-dev/*"
        }
    ]
}
POLICY
}
