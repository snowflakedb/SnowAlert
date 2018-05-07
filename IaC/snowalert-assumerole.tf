data "aws_s3_bucket" "sfc-snowalert-s3" {
  bucket = "sfc-snowalert-deploy"
}

resource "aws_lambda_function" "sfc-snowalert-assumerole-lambda" {
  function_name = "sfc-snowalert-assumerole-lambda"
  handler       = "snowalert-assumerole.lambda_handler"
  role          = "${aws_iam_role.sfc-snowalert-role.arn}"
  runtime       = "python3.6"
  s3_bucket     = "${aws_s3_bucket.sfc-snowalert-s3.id}"
  s3_key        = "snowalert-assumerole.zip"
  timeout       = "120"

  vpc_config = {
    subnet_ids         = ["subnet-09e219cf8f803d97a"]
    security_group_ids = ["sg-0f65fa84d7bdc98bb"]
  }

  environment {
    variables = {
      auth = "AQICAHgeGObvsFH0axIIfBAFflcHJOlt9LDwyCYJs4w3Jr9VKgECTNEgwZ1y00u/GPsWaa2JAAAAgzCBgAYJKoZIhvcNAQcGoHMwcQIBADBsBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDHFdGEhzt92fguNLlwIBEIA/Hnb+jnpkz6ZSiI1zfG7pLgE0jQvZDcPTuMNv4X79uJcMIXAM3Q5wT+CnDO4NCSGeUTqX0heH6krmQqyF2o7m"
    }
  }
}
