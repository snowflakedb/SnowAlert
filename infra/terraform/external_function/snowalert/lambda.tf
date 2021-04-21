data "archive_file" "lambda_code" {
  type        = "zip"
  source_dir  = "${path.module}/lambda-code"
  output_path = "${path.module}/lambda-code.zip"
  excludes = [
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
  ]
}

resource "aws_lambda_function" "stdefn" {
  function_name    = "${var.prefix}_external_function"
  role             = aws_iam_role.stdefn.arn
  handler          = "lambda_function.lambda_handler"
  memory_size      = "512"
  runtime          = "python3.8"
  timeout          = "300"
  publish          = null
  filename         = data.archive_file.lambda_code.output_path
  source_code_hash = data.archive_file.lambda_code.output_base64sha256
}

resource "aws_iam_role" "stdefn" {
  name = "${var.prefix}-external-function"
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
  policy_arn = aws_iam_policy.kms_decrypt.arn
}

resource "aws_lambda_permission" "api_gateway" {
  function_name = aws_lambda_function.stdefn.function_name
  principal     = "apigateway.amazonaws.com"
  action        = "lambda:InvokeFunction"
  source_arn    = "${aws_api_gateway_rest_api.ef_to_lambda.execution_arn}/*/*"

  depends_on = [aws_lambda_function.stdefn]
}
