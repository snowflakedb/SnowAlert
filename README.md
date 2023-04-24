[![CircleCI](https://circleci.com/gh/snowflakedb/SnowAlert.svg?style=svg)](https://circleci.com/gh/snowflakedb/SnowAlert)

<p align="center">
  <img height="300" src="https://raw.githubusercontent.com/snowflakedb/SnowAlert/master/docs/src/static/images/snowalert-logo.png">
</p>

SnowAlert is a security analytics framework that uses the Snowflake Cloud Data Platform to detect security incidents and policy violations.

## Data-Driven Security

At Snowflake, we switched from our old SIEM to putting all of our security logs and asset details into Snowflake.

Having our security data in Snowflake means that we can define advanced correlated alerts that are more reliable and less noisy.

We hope that Snowflake customers can do better data-driven security using Snowflake and that this project will make getting started easy.

Ready? Let's [get started!](https://docs.snowalert.com/ "SnowAlert Documentation")

## Native Snowflake features

Snowflake now supports native [Alerts and Notifications](https://docs.snowflake.com/guides-overview-alerts) which can be used for some generic alerting use-cases.

For other RPC's, you can also use [External Functions](https://docs.snowflake.com/en/sql-reference/external-functions-introduction) either manually created
or through a generic backend like [GEFF](https://github.com/Snowflake-Labs/geff/).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
