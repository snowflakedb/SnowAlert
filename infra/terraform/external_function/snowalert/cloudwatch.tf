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
          Resource = "arn:aws:logs:${data.aws_region.current.name}:${local.account_id}:*"
        },
        {
          Action = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
          ]
          Effect = "Allow"
          Resource = [
            "arn:aws:logs:us-west-2:${local.account_id}:log-group:/aws/lambda/${aws_lambda_function.stdefn.function_name}:*",
          ]
        },
        {
          Effect   = "Allow",
          Action   = "cloudwatch:PutMetricData",
          Resource = "*"
          Condition = {
            StringLike = {
              "cloudwatch:namespace" = var.aws_cloudwatch_metric_namespace
            }
          }
        },
      ]
    }
  )
}
