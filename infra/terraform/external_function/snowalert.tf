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

module "snowflake_api_integration_aws_gateway" {
  source                            = "./snowalert"
  prefix                            = "snowalert"
  snowflake_integration_external_id = var.snowflake_integration_external_id
  snowflake_integration_user        = var.snowflake_integration_user
  aws_cloudwatch_metric_namespace   = var.aws_cloudwatch_metric_namespace
}

variable "snowflake_api_integration_name" {
  type        = string
  description = "API Integration that can be used to make calls"
  default     = null
}

variable "aws_api_integration_assumed_role" {
  type        = string
  description = "IAM Role assumed by Snowflake to make calls"
  default     = "snowflake_api_integration_gateway_caller"
}

variable "aws_cloudwatch_metric_namespace" {
  type        = string
  description = "where EF can write CloudWatch Metrics"
  default     = "*"
}
