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
  aws_permission_boundry            = var.aws_permission_boundry
  aws_deployment_stage_name         = var.aws_deployment_stage_name
  gateway_logger_name               = var.gateway_logger_name
  gateway_caller_name               = var.gateway_caller_name
  aws_iam_role_policy_name          = var.aws_iam_role_policy_name
  snowflake_username                = var.snowflake_username
  snowflake_account                 = var.snowflake_account
  snowflake_password                = var.snowflake_password
  snowflake_role                    = var.snowflake_role
  prod_cloudwatch_write_name        = var.prod_cloudwatch_write_name
  kms_decrypt_name                  = var.kms_decrypt_name
  aws_lambda_function_name          = var.aws_lambda_function_name
}

variable "aws_permission_boundry" {
  type        = string
  default     = null
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

variable "prod_cloudwatch_write_name"{
  type        = string
  default     = "cloudwatch-setup-and-write1"
}

variable "kms_decrypt_name" {
  type        = string
  default     = "snowalert_kms_decrypt"
}

variable "aws_lambda_function_name" {
  type        = string
  default     = "snowalert_external_function1"
}