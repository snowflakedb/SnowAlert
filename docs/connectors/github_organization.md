## GitHub Webhooks to S3

In order to aggregate POST requests from GitHub to S3, infra must collect Webhooks to S3 before using the connector to Snowflake.
One way to aggregate POST requests, such as when a branch is deleted, a deploy key is added/removed from a repository,
or when a new download is created in GitHub, is to use the below archiver that sends these requests to an AWS API Gateway
which invokes a lambda. After the lambda processes the request, it is then sent to AWS Firehose to be formatted
to be put into S3.

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
