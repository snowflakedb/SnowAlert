variable "prefix" {}
variable "snowflake_integration_external_id" {}
variable "snowflake_integration_user" {}
variable "aws_cloudwatch_metric_namespace" {}

data "aws_caller_identity" "current" {}
locals {
  account_id = data.aws_caller_identity.current.account_id
}



