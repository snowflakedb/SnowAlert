variable "prefix" {}
# variable "snowflake_integration_external_id" {}
variable "snowflake_integration_user" {}
variable "aws_cloudwatch_metric_namespace" {}
variable "aws_permission_boundry" {}
variable "gateway_logger_name" {}
variable "gateway_caller_name" {}
variable "aws_iam_role_policy_name" {}
variable "aws_deployment_stage_name" {}
variable "snowflake_username" {}
variable "snowflake_account" {}
variable "snowflake_password" {}
variable "snowflake_role" {}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


locals {
  account_id  = data.aws_caller_identity.current.account_id
  region_name = data.aws_region.current.name
}
