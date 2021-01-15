/*

  logging API Gateway logging is set up per-region

*/

resource "aws_iam_role" "gateway_logger" {
  name = "${var.prefix}-api-gateway-logger"
  path = "/"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "gateway_logger" {
  role       = aws_iam_role.gateway_logger.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}

resource "aws_api_gateway_account" "api_gateway" {
  cloudwatch_role_arn = aws_iam_role.gateway_logger.arn
}

/*

  rest is API Gateway specific to External Functions

*/

resource "aws_iam_role" "gateway_caller" {
  name = "${var.prefix}-api-gateway-caller"
  path = "/"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_integration_external_id
          }
        }
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_integration_user
        }
      }
    ]
  })
}

resource "aws_api_gateway_rest_api" "ef_to_lambda" {
  name = "${var.prefix}-seceng-external-functions"
  endpoint_configuration {
    types = [
      "REGIONAL",
    ]
  }
}

resource "aws_api_gateway_rest_api_policy" "ef_to_lambda" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Principal = {
            AWS = "arn:aws:sts::${local.account_id}:assumed-role/${aws_iam_role.gateway_caller.name}/snowflake"
          }
          Action   = "execute-api:Invoke"
          Resource = "${aws_api_gateway_rest_api.ef_to_lambda.execution_arn}/*/POST/*"
        },
      ]
    }
  )
}

resource "aws_api_gateway_resource" "https" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "https"
}

resource "aws_api_gateway_resource" "smtp" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "smtp"
}

resource "aws_api_gateway_method" "https_post" {
  rest_api_id    = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id    = aws_api_gateway_resource.https.id
  http_method    = "POST"
  authorization  = "AWS_IAM"
  request_models = {}
  request_parameters = {
    "method.request.header.sf-custom-base-url"      = false
    "method.request.header.sf-custom-url"           = false
    "method.request.header.sf-custom-method"        = false
    "method.request.header.sf-custom-headers"       = false
    "method.request.header.sf-custom-params"        = false
    "method.request.header.sf-custom-data"          = false
    "method.request.header.sf-custom-json"          = false
    "method.request.header.sf-custom-timeout"       = false
    "method.request.header.sf-custom-auth"          = false
    "method.request.header.sf-custom-response-type" = false
  }
}

resource "aws_api_gateway_method" "smtp_post" {
  rest_api_id    = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id    = aws_api_gateway_resource.smtp.id
  http_method    = "POST"
  authorization  = "AWS_IAM"
  request_models = {}
  request_parameters = {
    "method.request.header.sf-custom-host"      = false
    "method.request.header.sf-custom-port"      = false
    "method.request.header.sf-custom-user"      = false
    "method.request.header.sf-custom-password"  = false
    "method.request.header.sf-custom-recipient" = false
    "method.request.header.sf-custom-subject"   = false
    "method.request.header.sf-custom-text"      = false
  }
}

resource "aws_api_gateway_integration" "https_to_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id             = aws_api_gateway_resource.https.id
  http_method             = aws_api_gateway_method.https_post.http_method
  integration_http_method = aws_api_gateway_method.https_post.http_method
  type                    = "AWS_PROXY"
  content_handling        = "CONVERT_TO_TEXT"
  timeout_milliseconds    = 29000
  uri                     = aws_lambda_function.stdefn.invoke_arn

  cache_key_parameters = null

  request_parameters = {}
  request_templates  = {}
}

resource "aws_api_gateway_integration" "smtp_to_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id             = aws_api_gateway_resource.smtp.id
  http_method             = aws_api_gateway_method.smtp_post.http_method
  integration_http_method = aws_api_gateway_method.smtp_post.http_method
  type                    = "AWS_PROXY"
  content_handling        = "CONVERT_TO_TEXT"
  timeout_milliseconds    = 29000
  uri                     = aws_lambda_function.stdefn.invoke_arn

  cache_key_parameters = null

  request_parameters = {}
  request_templates  = {}
}

resource "aws_cloudwatch_log_group" "api_gateway" {
  name = "API-Gateway-Execution-Logs_${aws_api_gateway_rest_api.ef_to_lambda.id}/prod"

  retention_in_days = 0 # never expire
}

resource "aws_api_gateway_stage" "prod" {
  depends_on = [aws_cloudwatch_log_group.api_gateway]

  stage_name    = "prod"
  rest_api_id   = aws_api_gateway_rest_api.ef_to_lambda.id
  deployment_id = aws_api_gateway_deployment.prod.id
}

resource "aws_api_gateway_deployment" "prod" {
  depends_on = [
    aws_api_gateway_integration.https_to_lambda,
    aws_api_gateway_integration.smtp_to_lambda
  ]

  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  stage_name  = "prod"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_method_settings" "enable_logging" {
  depends_on = [aws_api_gateway_account.api_gateway]

  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  stage_name  = aws_api_gateway_stage.prod.stage_name
  method_path = "*/*"
  settings {
    logging_level   = "INFO"
    metrics_enabled = true
  }
}
