.. image:: https://coveralls.io/repos/github/snowflakedb/SnowAlert/badge.svg?branch=master
:target: https://coveralls.io/github/snowflakedb/SnowAlert?branch=master

# SnowAlert

SnowAlert is a security analytics framework that uses the Snowflake data warehouse for identifying security incidents across diverse data sources and time ranges.

## Data-Driven Security

At Snowflake, we switched from our old SIEM to putting all of our security events, along with various report output, directly into Snowflake.

Having our security data in Snowflake means that we can query it in SQL using a server-less platform like AWS Lambda and get alerts that are more reliable and less noisy.

Developing for Snowflake has been easy using the existing Python connector and built-in support for JSON.

We hope that Snowflake customers can get better data-driven security by using Snowflake and that this project will make getting started easy.

If you use SnowAlert, we'd love to collaborate and make this a better tool for the whole Snowflake user community.

## Get Started

Check out the [user guide](https://snowalert.readthedocs.io) or reach out to the Snowflake security team at snowalert@snowflake.net.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
