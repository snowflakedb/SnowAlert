terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "3.34.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}



module "snowflake_api_integration_aws_gateway" {
  source                            = "./snowalert"
  prefix                            = "snowalert"
  snowflake_integration_user        = var.snowflake_integration_user
  aws_cloudwatch_metric_namespace   = var.aws_cloudwatch_metric_namespace
  aws_deployment_stage_name         = var.aws_deployment_stage_name
  gateway_logger_name               = var.gateway_logger_name
  gateway_caller_name               = var.gateway_caller_name
  aws_iam_role_policy_name          = var.aws_iam_role_policy_name
  snowflake_username                = var.snowflake_username
  snowflake_account                 = var.snowflake_account
  snowflake_password                = var.snowflake_password
  snowflake_role                    = var.snowflake_role
}

variable "snowflake_integration_user" {
  type        = string
  description = "user who will be calling the API Gateway"
  default     = null
}

variable "aws_cloudwatch_metric_namespace" {
  type        = string
  description = "where EF can write CloudWatch Metrics"
  default     = "*"
}

variable "gateway_logger_name" {
  type        = string
  default     = "gateway_logger1"
}

variable "gateway_caller_name" {
  type        = string
  default     = "gateway_caller1"
}

variable "aws_iam_role_policy_name" {
  type        = string
  default     = "gateway_policy1"
}

variable "aws_deployment_stage_name" {
  type        = string
  default     = "prod"
}

variable "snowflake_username" {
  type        = string
  default     = ""
}

variable "snowflake_account" {
  type        = string
  default     = ""
}

variable "snowflake_password" {
  type        = string
  default     = ""
}

variable "snowflake_role" {
  type        = string
  default     = ""
}