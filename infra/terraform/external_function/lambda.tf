resource "aws_lambda_function" "stdefn" {
  function_name = "${var.snowflake_account}_external_function"
  role          = aws_iam_role.stdefn.arn
  handler       = "lambda_function.lambda_handler"
  memory_size   = "512"
  runtime       = "python3.8"
  timeout       = "300"
  publish       = null
}

resource "aws_iam_role" "stdefn" {
  name = "${var.snowflake_account}_external_function-role"
  path = "/service-role/"

  assume_role_policy = jsonencode(
    {
      Version = "2012-10-17",
      Statement = [
        {
          Action = "sts:AssumeRole",
          Principal = {
            Service = "lambda.amazonaws.com"
          },
          Effect = "Allow"
        }
      ]
    }
  )
}

resource "aws_iam_role_policy_attachment" "standard_lib_write_logs" {
  role       = aws_iam_role.stdefn.name
  policy_arn = aws_iam_policy.prod_cloudwatch_write.arn
}
resource "aws_iam_role_policy_attachment" "standard_lib_decrypt_secrets" {
  role       = aws_iam_role.stdefn.name
  policy_arn = aws_iam_policy.prod_kms_decrypt.arn
}
