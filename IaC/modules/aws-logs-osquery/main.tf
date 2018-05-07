variable "aws_account_id" {}
variable "bucket_prefix_name" {}
variable "s3_bucket_arn" {}

variable "log_type" {
  default = "osquery"
}

variable "data_classification" {
  default = "unknown"
}

data "aws_iam_policy_document" "assumerole" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"

      identifiers = [
        "arn:aws:iam::${var.aws_account_id}:root",
      ]
    }
  }
}

resource "aws_iam_role" "s3_log_writer" {
  name               = "s3_log_writer-${var.log_type}-${var.bucket_prefix_name}"
  assume_role_policy = "${data.aws_iam_policy_document.assumerole.json}"
}

resource "aws_iam_role_policy" "s3_log_writer" {
  name = "s3_log_writer-${var.log_type}-${var.bucket_prefix_name}"
  role = "${aws_iam_role.s3_log_writer.id}"

  policy = "${data.template_file.s3_log_writer.rendered}"
}

data "template_file" "s3_log_writer" {
  template = "${file("${path.module}/../templates/s3_log_bucket_policy.iam.tpl")}"

  vars {
    s3_bucket_arn       = "${var.s3_bucket_arn}"
    data_classification = "${var.data_classification}"
    log_type            = "${var.log_type}"
    bucket_prefix_name  = "${var.bucket_prefix_name}"
  }
}
