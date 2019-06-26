variable "name" {}

variable "ecs_cpu" {
  default = 1024
}

variable "ecs_memory" {
  default = 2048
}

variable "task_definition_file" {}

variable "subnets" {
  type = "list"
}

variable "vpc_id" {}

variable "schedule_expression" {
  default = "cron(0 * * * ? *)"
}

variable "container_command" {
  default = "./run alerts"
}

variable "log_group" {
  default = "/ecs/scheduled_task/snowalert_run_alerts"
}

variable "kms_key_arn" {}

variable "additional_security_groups" {
  type    = "list"
  default = []
}
