resource "aws_iam_policy" "prod_cloudwatch_write" {
  name = "cloudwatch-setup-and-write"
  path = "/service-role/"

  policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Action   = "logs:CreateLogGroup"
          Effect   = "Allow"
          Resource = "arn:aws:logs:us-west-2:${var.aws_aid}:*"
        },
        {
          Action = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
          ]
          Effect = "Allow"
          Resource = [
            "arn:aws:logs:us-west-2:${var.aws_aid}:log-group:/aws/lambda/${aws_lambda_function.stdefn.function_name}:*",
          ]
        },
      ]
    }
  )
}
