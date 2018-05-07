module "aws-logs-syslog-sfc-dev" {
  source             = "modules/aws-logs-syslog"
  aws_account_id     = "185886769098"
  bucket_prefix_name = "sfc-dev"
  s3_bucket_arn      = "${aws_s3_bucket.logs.arn}"
}

module "aws-logs-osquery-sfc-dev" {
  source             = "modules/aws-logs-osquery"
  aws_account_id     = "185886769098"
  bucket_prefix_name = "sfc-dev"
  s3_bucket_arn      = "${aws_s3_bucket.logs.arn}"
}

module "aws-logs-nginx-sfc-dev" {
  source             = "modules/aws-logs-nginx"
  aws_account_id     = "185886769098"
  bucket_prefix_name = "sfc-dev"
  s3_bucket_arn      = "${aws_s3_bucket.logs.arn}"
}
