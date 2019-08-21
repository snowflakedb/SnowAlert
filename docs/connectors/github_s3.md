## Getting GitHub Webhooks into S3

GitHub allows Organization or Repository admins to set up webhooks for delivery to endpoints. Webhooks can be
triggered when

- a new commit is pushed
- a branch is deleted,
- a deploy key is added/removed from a repository, or
- a new download is created in GitHub.

SnowAlert does not yet natively support webhooks, or any other internet-facing infrastructure. Thus, a separate
component is necessary to collect GitHub webhooks to S3 before using this connector to Snowflake. One way to
accomplish this is to use the below archiver to send webhooks to an AWS API Gateway which invokes an AWS Lambda.
After the Lambda processes the request, it is then sent to AWS Firehose to be formatted and put into S3.

```
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

The firehose.tf also uses cztack/aws-s3-private-bucket which can be found in the cztack repo.github_organizations.
By using this, we can then send data from S3 to Snowflake easily using the connector created.
