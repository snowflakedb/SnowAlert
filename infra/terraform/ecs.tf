resource "aws_ecs_cluster" "snowalert_cluster" {
  name = "${var.name}-snowalert"
}

resource "aws_ecs_task_definition" "snowalert_runner" {
  family                   = "${var.name}-snowalert_runner"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "${var.ecs_cpu}"
  memory                   = "${var.ecs_memory}"
  task_role_arn            = "${aws_iam_role.ecs_task_execution.arn}"
  execution_role_arn       = "${aws_iam_role.ecs_task_execution.arn}"
  container_definitions    = "${var.task_definition_file}"
}
