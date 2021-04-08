# resource "snowflake_external_function" "test_ext_func" {
#   # depends_on = [snowflake_api_integration.api_integration,
#   #               aws_api_gateway_deployment.prod,
#   #               aws_api_gateway_rest_api_policy.ef_to_lambda
#   #             ]
#   name       = "${var.prefix}_external_function"
#   database   = "NEW_DB"
#   schema     = "PUBLIC"
#   # arg {
#   #   name = "event"
#   #   type = "varchar"
#   # }
#   #   arg {
#   #     name = "arg2"
#   #     type = "varchar"
#   #   }
#   return_type     = "VARIANT"
#   return_behavior = "VOLATILE"
#   api_integration = snowflake_api_integration.api_integration.name
#   header {
#     name  = "url"
#     value = "https://ip-ranges.amazonaws.com/ip-ranges.json"
#   }
#   url_of_proxy_and_resource = "${aws_api_gateway_deployment.prod.invoke_url}https"
# }
