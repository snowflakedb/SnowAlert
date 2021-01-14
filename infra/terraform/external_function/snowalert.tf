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
  snowflake_integration_external_id = ["aaa"]
  snowflake_integration_user        = ["arn:aws:iam::bbb:user/c_snowhouse_stage_volume"]
}
