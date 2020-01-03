## Cloudformation template

The template found inside `snowalert.yaml` contains the equivalent infrastructure produced by the k8s/terraform templates, but expressed natively for AWS.

As such, it creates a fargate ECS cluster for the SnowAlert alert mechanism (using cloudwatch cron trigger). It optionally creates another fargate cluster for the SnowAlert Web UI, complete with an application load balancer, certificate, and Route53 DNS record.

To prevent the creation of the SnowAlert Web UI, set `DeployWebUi` to "false".

The most notable difference is that secrets are retrieved by ECS from AWS SSM rather than passed in as parameters.

Prerequisites:
1) If the web UI is being deployed, a route 53 hosted zone and ACS certificate must already exist.
2) The ID of a VPC and two subnets must be known prior.
3) You must have stored your SnowAlert configuration in the following SSM parameters (change the values for "SnowAlert" or "master" if you plan to override ResourcesPrefix or Slice):
```
/SnowAlert/master/PRIVATE_KEY
/SnowAlert/master/PRIVATE_KEY_PASSWORD
/SnowAlert/master/SLACK_API_TOKEN
/SnowAlert/master/JIRA_PASSWORD
/SnowAlert/master/OAUTH_CLIENT_(snowflake_account_id)
/SnowAlert/master/OAUTH_SECRET_(snowflake_account_id)
```
_Note: The (snowflake_account_id) suffix from those last two parameters is set by the SnowflakeAccount cloudformation parameter_

For optional parameters like `SLACK_API_TOKEN` and `JIRA_PASSWORD`, you must still set them but just leave them blank. ECS does not support optional secrets and Cloudformation cannot provide them conditionally.

The remaining non-sensitive parameters (`SA_USER`,`SA_WAREHOUSE`,`SA_DATABASE`,`SA_ROLE`,`SNOWFLAKE_ACCOUNT`,`JIRA_USER`,`JIRA_URL`,`JIRA_PROJECT`) are configured as regular environment variables fed by Cloudformation parameters.

Assuming the AWS CLI has been configured with default region and credentials, for a role similar to Power User, you can provision the stack from the commandline like so.

No Web UI:
```
aws cloudformation create-stack --template-body snowalert.yaml --stack-name SnowAlert-Prod --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Vpc,ParameterValue=vpc-a1234567 ParameterKey=SubnetOne,ParameterValue=subnet-1ab23456 ParameterKey=SubnetTwo,ParameterValue=subnet-2cd34567 ParameterKey=DeployWebUi,ParameterValue=false ParameterKey=SnowflakeAccount,ParameterValue=ab1234567 
```

With Web UI, internal VPC access only:
```
aws cloudformation create-stack --template-body snowalert.yaml --stack-name SnowAlert-Prod --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Vpc,ParameterValue=vpc-a1234567 ParameterKey=SubnetOne,ParameterValue=subnet-1ab23456 ParameterKey=SubnetTwo,ParameterValue=subnet-2cd34567 ParameterKey=CertificateArn,ParameterValue=arn:aws:acm:ap-southeast-2:123456789012:certificate/461b5dc1-443c-46ef-a5df-a6e6a7190d67 ParameterKey=InstanceFqdn,ParameterValue=snowalert.myinternaldomain.com ParameterKey=InstanceHostedZone,ParameterValue=myinternaldomain.com. ParameterKey=SnowflakeAccount,ParameterValue=ab1234567 
```

With Web UI, public facing and including Jira integration:
```
aws cloudformation create-stack --template-body snowalert.yaml --stack-name SnowAlert-Prod --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Vpc,ParameterValue=vpc-a1234567 ParameterKey=SubnetOne,ParameterValue=subnet-1ab23456 ParameterKey=SubnetTwo,ParameterValue=subnet-2cd34567 ParameterKey=CertificateArn,ParameterValue=arn:aws:acm:ap-southeast-2:123456789012:certificate/461b5dc1-443c-46ef-a5df-a6e6a7190d67 ParameterKey=InstanceFqdn,ParameterValue=snowalert.mydomain.com ParameterKey=InstanceHostedZone,ParameterValue=mydomain.com. ParameterKey=WebUiAlbScheme,ParameterValue=internet-facing ParameterKey=SnowflakeAccount,ParameterValue=ab1234567 ParameterKey=SnowAlertJiraUser,ParameterValue=snowalertuser ParameterKey=SnowAlertJiraUrl,ParameterValue=https://mydomain.atlassian.net/ ParameterKey=SnowAlertJiraProject,ParameterValue=SA 
```

