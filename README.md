[![CircleCI](https://circleci.com/gh/snowflakedb/SnowAlert.svg?style=svg)](https://circleci.com/gh/snowflakedb/SnowAlert)

<p align="center">
  <img height="300" src="https://raw.githubusercontent.com/snowflakedb/SnowAlert/master/docs/src/static/images/snowalert-logo.png">
</p>

SnowAlert was a security analytics framework that uses the Snowflake Cloud Data Platform to detect security incidents and policy violations. It is now deprecated.

## Native Snowflake features and implementation

Snowflake now supports native [Alerts and Notifications](https://docs.snowflake.com/guides-overview-alerts) which can be used for some generic alerting use-cases.

For other RPC's, you can also use [External Functions](https://docs.snowflake.com/en/sql-reference/external-functions-introduction) either manually created
or through a generic backend like [GEFF](https://github.com/Snowflake-Labs/geff/).

An partial re-implementation of SnowAlert in JavaScript using Snowflake Tasks and External Functions via GEFF and deployed using Terraform is in [Snowflake-Labs/terraform-snowflake-snowalert](https://github.com/Snowflake-Labs/terraform-snowflake-snowalert/).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
