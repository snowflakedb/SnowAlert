resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.name}-ecsTaskExecutionRole"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_policy" "snowalert_execution" {
  name        = "${var.name}-snowalert_execution"
  description = "${var.name} SnowAlert execution policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": {
    "Action": [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "cloudwatch:PutMetricData"
    ],
    "Effect": "Allow",
    "Resource": "*"
  }
}
EOF
}

resource "aws_iam_policy_attachment" "snowalert_execution" {
  name       = "snowalert_log_attachment"
  roles      = ["${aws_iam_role.ecs_task_execution.name}"]
  policy_arn = "${aws_iam_policy.snowalert_execution.arn}"
}

resource "aws_iam_policy" "snowalert_decrypt" {
  name        = "${var.name}-snowalert_decrypt"
  description = "${var.name} SnowAlert decrypt policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": {
    "Effect": "Allow",
    "Action": "kms:Decrypt",
    "Resource": "${var.kms_key_arn}"
  }
}
EOF
}

resource "aws_iam_policy_attachment" "snowalert_decrypt" {
  name       = "snowalert_decrypt_attachment"
  roles      = ["${aws_iam_role.ecs_task_execution.name}"]
  policy_arn = "${aws_iam_policy.snowalert_decrypt.arn}"
}

resource "aws_iam_role" "ecs_run_task" {
  name = "${var.name}-ecsRunTaskRole"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "snowalert_runtask" {
  name        = "${var.name}-snowalert_runtask"
  description = "${var.name} SnowAlert run task policy"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask"
      ],
      "Resource": [
        "${aws_ecs_task_definition.snowalert_runner.arn}"
      ],
      "Condition": {
        "ArnLike": {
          "ecs:cluster": "${aws_ecs_cluster.snowalert_cluster.arn}"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "*"
      ],
      "Condition": {
        "StringLike": {
          "iam:PassedToService": "ecs-tasks.amazonaws.com"
        }
      }
    }
  ]
}
EOF
}
resource "aws_iam_policy_attachment" "snowalert_runtask" {
  name       = "snowalert_runtask"
  roles      = ["${aws_iam_role.ecs_run_task.name}"]
  policy_arn = "${aws_iam_policy.snowalert_runtask.arn}"
}
