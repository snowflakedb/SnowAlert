resource "aws_cloudwatch_log_group" "snowalert_logs" {
  name = "${var.log_group}"
}

resource "aws_cloudwatch_event_rule" "snowalert_daily" {
  name                = "${var.name}_snowalert_daily_rule"
  description         = "${var.name} Run Alerts Daily"
  schedule_expression = "${var.schedule_expression}"
}

resource "aws_cloudwatch_event_target" "snowalert_daily" {
  target_id = "${var.name}-snowalert"
  arn       = "${aws_ecs_cluster.snowalert_cluster.id}"
  rule      = "${aws_cloudwatch_event_rule.snowalert_daily.name}"
  role_arn  = "${aws_iam_role.ecs_run_task.arn}"

  ecs_target = {
    task_count          = 1
    launch_type         = "FARGATE"
    task_definition_arn = "${aws_ecs_task_definition.snowalert_runner.arn}"

    network_configuration = {
      subnets          = ["${var.subnets}"]
      security_groups  = ["${concat(list(aws_security_group.snowalert.id), var.additional_security_groups)}"]
      assign_public_ip = true
    }
  }

  input = <<DOC
{
  "containerOverrides": [
    {
      "name": "snowalert_container",
      "command": ["${var.container_command}"]
    }
  ]
}
DOC
}
