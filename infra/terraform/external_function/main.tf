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

variable "aws_aid" {
  type        = string
  description = "AWS Account Id that will have API Gateway, Lambda"
  default     = "nnn"
}

variable "snowflake_account" {
  type        = string
  description = "the name of the snowflake account"
  default     = "snowhouse"
}

variable "snowflake_integration_external_id" {
  type        = string
  description = "allows Snowflake to assume the API Gateway caller role"
  default     = "aaa_SFCRole=bbb="
}

variable "snowflake_integration_user" {
  type        = string
  description = "allows Snowflake to assume the API Gateway caller role"
  default     = "arn:aws:iam::ccc:user/c_ddd_stage_volume"
}
