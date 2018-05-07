resource "aws_iam_user" "logs_sfc-security" {
  name = "logs_sfc-security"
}

resource "aws_iam_user_policy" "logs_sfc-security" {
  name = "logs_sfc-security"
  user = "${aws_iam_user.logs_sfc-security.name}"

  policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": "${aws_s3_bucket.logs.arn}/aws/sfc-security/*"
        }
    ]
}
POLICY
}
