## Salesforce Event Log Connector

In order to use the Salesforce Event Log connector, you must create a user account that has permission to access event log files as well as API access, as described [here](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/event_log_file_hourly_overview.htm). There is also a [Trailhead module](https://trailhead.salesforce.com/en/content/learn/modules/event_monitoring/event_monitoring_query) which covers this.

It is recommended that the user account be set up in the style of an "Integration User", where API access is the only access they have (i.e. they do not have permission to sign in to the Lightning Experience or view Accounts, Contacts etc, and their password is set to never expire). This type of setup is described [here](https://help.salesforce.com/articleView?id=000331470&type=1&mode=1).

You will need to obtain or reset the user's security token as described [here](https://help.salesforce.com/articleView?id=user_security_token.htm)
