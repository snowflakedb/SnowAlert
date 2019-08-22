## Getting GitHub Webhooks into S3

GitHub allows Organization or Repository admins to set up webhooks for delivery to endpoints. [Webhooks](https://developer.github.com/v3/activity/events/types/) can be
triggered when

- a new commit is pushed
- a branch is deleted,
- a deploy key is added/removed from a repository,
- a new download is created in GitHub,
- GitHub package vulnerability alerts, or
- [many others](https://developer.github.com/v3/activity/events/types/)

SnowAlert does not yet natively support webhooks, or any other internet-facing infrastructure. Thus, a separate
component is necessary to collect GitHub webhooks to S3 before using this connector to Snowflake. One way to
accomplish this is to use the [below archiver](https://github.com/chanzuckerberg/cztack/tree/master/github-webhooks-to-s3#github-webhooks-to-s3) to send webhooks to an AWS API Gateway which invokes an AWS Lambda.
After the Lambda processes the request, it is then sent to AWS Firehose to be formatted and put into S3.

```hcl
module "archiver" {
  source = "github.com/chanzuckerberg/cztack//aws-acm-certgithub-webhooks-to-s3?ref=v0.19.0"

  env     = "${var.env}"
  project = "${var.project}"
  owner   = "${var.owner}"
  service = "${var.component}"

  fqdn            = "..."
  certificate_arn = "..."
  route53_zone_id = "..."
}
```

