## Manual Steps on Google Cloud Platform (console.cloud.google.com)

1.  Make sure your Google Cloud user principal has the Editor role.
2.  Enable G Suite Domain-wide Delegration on the auditor service account.
3.  Enter "G Suite Integration" on the "Product name for the consent screen" field.
4.  The email address should be your login user for the current organization.
5.  Click the "View Client ID" link on the auditor service account.  Set
    the "Display name" on the "Client ID for service account" hover to be
    "G Suite Integration".
6.  Navigate to the "Credentials" page under "API & Services".  Click on the "OAuth consent screen".
    Application type = Internal
    Application name = G Suite Integration
    Support email    = <your login user email for the current organization>

## Manual Steps on Cloud Identity (admin.google.com)

1.  Navigate to *Security* > *Advanced settings* > *Manage API client access*
    Client Name            = **`<Client ID of the auditor service account>`**
    One or More API Scopes =
~~~
  [
      https://www.googleapis.com/auth/admin.reports.audit.readonly
      https://www.googleapis.com/auth/admin.reports.usage.readonly
      https://www.googleapis.com/auth/admin.directory.orgunit.readonly
      https://www.googleapis.com/auth/admin.directory.group.member.readonly
      https://www.googleapis.com/auth/admin.directory.group.readonly
      https://www.googleapis.com/auth/admin.directory.user.readonly
      https://www.googleapis.com/auth/admin.directory.user.alias.readonly
      https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly
  ]
~~~
    <Hit the Authorize button>
