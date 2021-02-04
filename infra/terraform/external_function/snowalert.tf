terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

module "snowalert" {
  source                            = "./snowalert"
  prefix                            = "snowalert"
  snowflake_integration_external_id = var.snowflake_integration_external_id
  snowflake_integration_user        = var.snowflake_integration_user
  aws_cloudwatch_metric_namespace   = var.aws_cloudwatch_metric_namespace
}

variable "snowflake_integration_user" {
  type        = string
  description = "user who will be calling the API Gateway"
  default     = "aaa"
}

variable "snowflake_integration_external_id" {
  type        = string
  description = "API_AWS_EXTERNAL_ID from DESC INTEGRATION ..."
  default     = "bbb"
}

variable "aws_cloudwatch_metric_namespace" {
  type        = string
  description = "where EF can write CloudWatch Metrics"
  default     = "*"
}
