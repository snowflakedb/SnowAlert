## GitHub Connector

When creating a new connector, add connector name to __init__.py and to src/connectors.

Within the src/connectors file, the landing table columns are labeled in the `LANDING_TABLE_COLUMNS` list which gives each
column variable a variable type. There are two functions that create the connector:

The connect function has parameters, and one of them is `connection_name` which is a name you get to choose,
make sure it is unique. We use "default" as the name. The other parameter is `options` which is a dict with keys
`bucket_name` and `aws_role`. The `bucket_name` can be found in the S3 console. The `aws_role` can be found after
running `make apply` in whatever directory is creating the roles. The result of running the connect function is a dict
that contains the role and the ExternalId that you should verify are the correct values.

The finalize function has a parameter called `connection_name` which should be the same value as the `connection_name`
parameter for the connect function. The result of calling this function is a message that suggests to run:
```
ALTER PIPE data.GITHUB_ORGANIZATIONS_DEFAULT_EVENTS_PIPE REFRESH;
```
in SnowFlake.

## GitHub Webhooks to S3

One way to aggregate POST requests in GitHub is using the below archiver that sends these requests to an AWS
API Gateway which invokes a lambda. After the lambda processes the request, it is then sent to AWS Firehose to be
formatted to be put into S3. You can find the Webhooks code here: https://github.com/chanzuckerberg/cztack

```
module "archiver" {
  source = "github.com/chanzuckerberg/cztack/github-webhooks-to-s3"

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
By using this, we can then send data from S3 to SnowFlake easily using the connector created.