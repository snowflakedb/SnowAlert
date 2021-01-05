resource "aws_iam_role" "gateway_caller" {
  name = "${var.snowflake_account}-api-gateway-caller"
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
  name = "${var.snowflake_account}-seceng-external-functions"
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
          Action = "execute-api:Invoke"
          Effect = "Allow"
          Principal = {
            AWS = "arn:aws:sts::${var.aws_aid}:assumed-role/${aws_iam_role.gateway_caller.name}/snowflake"
          }
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

resource "aws_api_gateway_integration" "to_lambda" {
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
