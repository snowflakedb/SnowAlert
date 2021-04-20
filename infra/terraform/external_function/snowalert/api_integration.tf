terraform {
  required_providers {
    snowflake = {
          source = "chanzuckerberg/snowflake"
          version = "0.24.0"
        }
  }
}

provider "snowflake" {
  username = var.snowflake_username   
  account  = var.snowflake_account    
  password = var.snowflake_password   
  role     = var.snowflake_role       
}

resource "snowflake_api_integration" "api_integration" {
  name    = "${var.prefix}_api_integration"
  enabled = true
  api_provider = "aws_api_gateway"
  api_allowed_prefixes = [aws_api_gateway_deployment.prod.invoke_url]
  api_aws_role_arn     = "arn:aws:iam::${local.account_id}:role/${var.prefix}-api-gateway-caller"
}

