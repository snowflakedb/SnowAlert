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
          AWS = coalesce(var.snowflake_integration_user, "arn:aws:iam::${local.account_id}:root")
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "gateway_caller" {
  name = "gateway_caller"
  role = aws_iam_role.gateway_caller.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "execute-api:Invoke"
        Resource = "${aws_api_gateway_rest_api.ef_to_lambda.execution_arn}/*/POST/*"
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
    "method.request.header.sf-custom-verbose"       = false
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

resource "aws_api_gateway_resource" "smtp" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "smtp"
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

resource "aws_api_gateway_resource" "cloudwatch_metric" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "cloudwatch_metric"
}

resource "aws_api_gateway_method" "cloudwatch_metric_post" {
  rest_api_id    = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id    = aws_api_gateway_resource.cloudwatch_metric.id
  http_method    = "POST"
  authorization  = "AWS_IAM"
  request_models = {}
  request_parameters = {
    "method.request.header.sf-custom-namespace"  = false
    "method.request.header.sf-custom-name"       = false
    "method.request.header.sf-custom-dimensions" = false
    "method.request.header.sf-custom-unit"       = false
    "method.request.header.sf-custom-value"      = false
    "method.request.header.sf-custom-region"     = false
    "method.request.header.sf-custom-timestamp"  = false
  }
}

resource "aws_api_gateway_integration" "cloudwatch_metric_to_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id             = aws_api_gateway_resource.cloudwatch_metric.id
  http_method             = aws_api_gateway_method.cloudwatch_metric_post.http_method
  integration_http_method = aws_api_gateway_method.cloudwatch_metric_post.http_method
  type                    = "AWS_PROXY"
  content_handling        = "CONVERT_TO_TEXT"
  timeout_milliseconds    = 29000
  uri                     = aws_lambda_function.stdefn.invoke_arn

  cache_key_parameters = null

  request_parameters = {}
  request_templates  = {}
}

resource "aws_api_gateway_resource" "boto3" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "boto3"
}

resource "aws_api_gateway_method" "boto3_post" {
  rest_api_id    = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id    = aws_api_gateway_resource.boto3.id
  http_method    = "POST"
  authorization  = "AWS_IAM"
  request_models = {}
  request_parameters = {
    "method.request.header.sf-custom-Namespace"     = false
    "method.request.header.sf-custom-MetricData"    = false
    "method.request.header.sf-custom-logGroupName"  = false
    "method.request.header.sf-custom-logStreamName" = false
  }
}

resource "aws_api_gateway_integration" "boto3_to_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id             = aws_api_gateway_resource.boto3.id
  http_method             = aws_api_gateway_method.boto3_post.http_method
  integration_http_method = aws_api_gateway_method.boto3_post.http_method
  type                    = "AWS_PROXY"
  content_handling        = "CONVERT_TO_TEXT"
  timeout_milliseconds    = 29000
  uri                     = aws_lambda_function.stdefn.invoke_arn

  cache_key_parameters = null

  request_parameters = {}
  request_templates  = {}
}

resource "aws_api_gateway_resource" "xmlrpc" {
  rest_api_id = aws_api_gateway_rest_api.ef_to_lambda.id
  parent_id   = aws_api_gateway_rest_api.ef_to_lambda.root_resource_id
  path_part   = "xml-rpc"
}

resource "aws_api_gateway_method" "xmlrpc_post" {
  rest_api_id    = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id    = aws_api_gateway_resource.xmlrpc.id
  http_method    = "POST"
  authorization  = "AWS_IAM"
  request_models = {}
  request_parameters = {
    "method.request.header.sf-custom-url" = false
  }
}

resource "aws_api_gateway_integration" "xmlrpc_to_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.ef_to_lambda.id
  resource_id             = aws_api_gateway_resource.xmlrpc.id
  http_method             = aws_api_gateway_method.xmlrpc_post.http_method
  integration_http_method = aws_api_gateway_method.xmlrpc_post.http_method
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

  stage_name    = aws_api_gateway_deployment.prod.stage_name
  rest_api_id   = aws_api_gateway_rest_api.ef_to_lambda.id
  deployment_id = aws_api_gateway_deployment.prod.id
}

resource "aws_api_gateway_deployment" "prod" {
  depends_on = [
    aws_api_gateway_integration.https_to_lambda,
    aws_api_gateway_integration.smtp_to_lambda,
    aws_api_gateway_integration.boto3_to_lambda,
    aws_api_gateway_integration.xmlrpc_to_lambda,
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
    logging_level          = "INFO"
    metrics_enabled        = true
    throttling_burst_limit = 5000
    throttling_rate_limit  = 10000
  }
}
