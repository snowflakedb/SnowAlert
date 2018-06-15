import sys
import os
import getpass
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from subprocess import call

import snowflake.connector

ACCOUNT_ADMIN_QUERY = "use role accountadmin;"
ROLE_CREATION_QUERY = "create role if not exists SNOWALERT;"
USER_CREATION_QUERY = "create user if not exists snowalert login_name = 'snowalert' password = '' default_role='SNOWALERT' must_change_password=false;"
ROLE_GRANT_QUERY = "grant role SNOWALERT to user snowalert;"

GRANT_PRIVILEGES_SCHEMA_QUERY = "grant all privileges on all schemas in database snowalert to role snowalert;"
GRANT_USAGE_QUERY = "GRANT USAGE ON WAREHOUSE snowalert TO ROLE snowalert;"

SET_DEFAULT_WAREHOUSE_QUERY = "alter user snowalert set default_warehouse=snowalert;"
USE_WAREHOUSE_QUERY = "use warehouse snowalert;"

WAREHOUSE_CREATION_QUERY = "create warehouse if not exists snowalert warehouse_size = xsmall warehouse_type = standard auto_suspend = 60 auto_resume = true initially_suspended = true;"
DATABASE_CREATION_QUERY = "create database if not exists snowalert;"
USE_DATABASE_QUERY = "use database snowalert;"
GRANT_PRIVILEGES_WAREHOUSE_QUERY = "grant all privileges on warehouse snowalert to role SNOWALERT;"
GRANT_PRIVILEGES_DATABASE_QUERY = "grant all privileges on database snowalert to role SNOWALERT;"
CREATE_ALERTS_TABLE_QUERY = "create table if not exists alerts ( alert variant, ticket string, suppressed boolean, suppression_rule string default null, counter integer default 1 );"
CREATE_QUERIES_TABLE_QUERY = "create table if not exists snowalert_queries ( query_spec variant );"
GRANT_PRIVILEGES_ALERTS_QUERY = "grant all privileges on table alerts to role SNOWALERT;"
GRANT_PRIVILEGES_QUERIES_QUERY = "grant all privileges on table snowalert_queries to role SNOWALERT;"
CREATE_SUPPRESSIONS_TABLE_QUERY = "create table if not exists suppression_queries ( suppression_spec variant );"
GRANT_PRIVILEGES_SUPPRESSIONS_QUERY = "grant all privileges on table suppression_queries to role SNOWALERT;"

INSERT_SAMPLE_QUERY_SPEC_QUERY= "insert into snowalert_queries select parse_json(column1) from values('{}');"
INSERT_SAMPLE_SUPPRESSION_SPEC_QUERY = "insert into suppression_queries select parse_json(column1) from values('{}');"


def login():
    print("This is the installer for SnowAlert; it will set up all of the resources needed for SnowAlert to run.")
    print("You will be prompted for several pieces of information during this process, including a Snowflake username, Snowflake account, and two different passwords")
    account = input("Please enter the Snowflake account name where you want to configure SnowAlert: ")
    print("Please enter the username of the user you would like to use to configure SnowAlert. This user should be able to use the 'accountadmin' role in your Snowflake account")
    username = input("Snowflake username: ")
    password = getpass.getpass("Please enter the password for the user you provided above: ")

    # We need to pass the account name in to the terraform configuration 
    # file as an environmental variable, because the lambdas will expect it. Writing the account name to a file is
    # an easy way to pass information between this script and terraform

    #f = open('account.txt', 'w')
    #f.write(account)
    #f.close()

    print("Authenticating to Snowflake...")
    try:
        ctx = snowflake.connector.connect(user=username, account=account, password=password)
    except Exception as e:
        print("Snowflake connection failed with error {}".format(e))
        sys.exit(1)

    print("Authentication successful")
    return ctx, account 


def warehouse_setup(ctx):
    
    print("Creating warehouse...")
    try:
        ctx.cursor().execute(WAREHOUSE_CREATION_QUERY)
    except Exception as e:
        print("Failed to create the warehouse with error {}".format(e))
        sys.exit(1)
    print("Warehouse creation successful")

    print("Setting default warehouse...")
    try:
        ctx.cursor().execute(SET_DEFAULT_WAREHOUSE_QUERY)
    except Exception as e:
        print("Failed to set the default warehouse with error {}".format(e))
        sys.exit(1)
    print("Successfully set the default warehouse for the user")

    print("Using warehouse...")
    try:
        ctx.cursor().execute(USE_WAREHOUSE_QUERY)
    except Exception as e:
        print("Failed to use warehouse with error {}".format(e))
        sys.exit(1)
    print("Now using the warehouse created")

    print("Creating database...")
    try:
        ctx.cursor().execute(DATABASE_CREATION_QUERY)
    except Exception as e:
        print("Failed to create the database with error {}".format(e))
        sys.exit(1)
    print("Database creation successful")

    print("Using database...")
    try:
        ctx.cursor().execute(USE_DATABASE_QUERY)
    except Exception as e:
        print("Failed to use database with error {}".format(e))
        sys.exit(1)
    print("Now using the database created...")

    print("Granting privileges on warehouse to role...")
    try:
        ctx.cursor().execute(GRANT_PRIVILEGES_WAREHOUSE_QUERY)
    except Exception as e:
        print("Failed to grant privileges on the warehouse with error {}".format(e))
        sys.exit(1)
    print("Warehouse privileges granted successfully")

    print("Granting usage on warehouse to role...")
    try:
        ctx.cursor().execute(GRANT_USAGE_QUERY)
    except Exception as e:
        print("Failed to grant useage on the warehouse with error {}".format(e))
        sys.exit(1)
    print("Warehouse usage granted successfully")

    print("Granting privileges on database to role...")
    try:
        ctx.cursor().execute(GRANT_PRIVILEGES_DATABASE_QUERY)
    except Exception as e:
        print("Failed to grant privileges on the database with error {}".format(e))
        sys.exit(1)
    print("Database privileges granted successfully")

    print("Granting privileges on schemas in database to role..")
    try:    
        ctx.cursor().execute(GRANT_PRIVILEGES_SCHEMA_QUERY)
    except Exception as e:
        print("Failed to grant privileges on the schemas in the database with error {}".format(e))
        sys.exit(1)
    print("Schema privileges granted successfully")

    print("Creating alerts table...")
    try:
        ctx.cursor().execute(CREATE_ALERTS_TABLE_QUERY)
    except Exception as e:
        print("Failed to create alerts table with error {}".format(e))
        sys.exit(1)
    print("Alerts table created successfully")

    print("Creating query table...")
    try:
        ctx.cursor().execute(CREATE_QUERIES_TABLE_QUERY)
    except Exception as e:
        print("Failed to create query table with error {}".format(e))
        sys.exit(1)
    print("Query table created successfully")

    print("Creating suppression table...")
    try:
        ctx.cursor().execute(CREATE_SUPPRESSIONS_TABLE_QUERY)
    except Exception as e:
        print("Failed to create suppression table with error {}".format(e))
        sys.exit(1)
    print("Suppression table created successfully")

    print("Granting privileges on alerts table to role...")
    try:
        ctx.cursor().execute(GRANT_PRIVILEGES_ALERTS_QUERY)
    except Exception as e:
        print("Failed to grant privileges on alerts table with error {}".format(e))
        sys.exit(1)
    print("Privileges on alerts table granted successfully")

    print("Granting privileges on queries table to role...")
    try:
        ctx.cursor().execute(GRANT_PRIVILEGES_QUERIES_QUERY)
    except Exception as e:
        print("Failed to grant privileges on queries table with error {}".format(e))
        sys.exit(1)
    print("Privileges on queries table granted successfully")

    print("Granting privileges on suppressions table to role...")
    try:
        ctx.cursor().execute(GRANT_PRIVILEGES_SUPPRESSIONS_QUERY)
    except Exception as e:
        print("Failed to grant privileges on suppressions table with error {}".format(e))
        sys.exit(1)
    print("Privileges on suppressions table granted successfully")


def setup_keypair(ctx):
    print("Creating the private key for the SnowAlert user! Please pick a strong password to protect this key.") 
    print("This password will be used to encrypt the private key used for key-pair authentication to Snowflake.")
    print("You will need to type this four times during the installation process, but afterwards it will be encrypted")
    print("with a KMS key and the encrypted value written to disk. This encrypted value will be used as an environemntal")
    print("variable for Lambdas which require it; you will not be required to type this password in order")
    print("to run the lamba functions themselves.")

    # We run openssl in a docker container so that we can be guaranteed it's available.  
    success = 1
    while success == 1:
        success = call("docker run --rm -it --mount type=bind,source=\"$(pwd)\",target=/var/task lambci/lambda:build-python3.6 ./privatekey.sh", shell=True)

    print("Public key saved as rsa_key.pub")
    f = open("rsa_key.pub", "r")
    f.readline()
    key = ""
    buffer = ""
    while buffer != '-----END PUBLIC KEY-----\n':
        buffer = f.readline()
        if buffer != '-----END PUBLIC KEY-----\n':
            key = key + buffer

    return key


def setup_user(ctx):
    print("Starting user setup...")
    # First step: use accountadmin
    try:
        ctx.cursor().execute(ACCOUNT_ADMIN_QUERY)
    except Exception as e:
        print("Failed to use accountadmin with error {}".format(e))
        sys.exit(1)
    print("Using accountadmin to configure Snowflake")
    print("Starting role creation...")
    # We need to create the role
    try:
        ctx.cursor().execute(ROLE_CREATION_QUERY)
    except Exception as e:
        print("Role creation failed with error {}".format(e))
        sys.exit(1)
    print("SnowAlert role created successfully")
    print("Starting user creation...")

    # Now we create the user
    try:
        ctx.cursor().execute(USER_CREATION_QUERY)
    except Exception as e:
        print("User creation failed with error {}".format(e))
        sys.exit(1)

    print("SnowAlert user created successfully")
    print("Granting SnowAlert role to SnowAlert user...")

    try:
        ctx.cursor().execute(ROLE_GRANT_QUERY)
    except Exception as e:
        print("Failed to grant the role to the SnowAlert user with error {}".format(e))
        sys.exit(1)

    print("Granted role successfully")

    print("Creating the public and private keypairs for SnowAlert...")
    key = setup_keypair(ctx)
    
    SET_PUBLIC_KEY_QUERY = "alter user snowalert set rsa_public_key='"+key+"';"
    print("Associating public key with SnowAlert user...")
    try:
        ctx.cursor().execute(SET_PUBLIC_KEY_QUERY)
    except Exception as e:
        print("Failed to associate the public key with the SnowAlert user with error {}".format(e))
        sys.exit(1)

    print("SnowAlert user has the public key")

def query_setup(ctx):
    print("Inserting a sample query into the query spec table...")
    with open("sample_query.qs", "r") as qs:
        query_spec = qs.read()
    try:
        ctx.cursor().execute(INSERT_SAMPLE_QUERY_SPEC_QUERY.format(query_spec))
    except Exception as e:
        print("Failed to insert the sample query with error {}".format(e))
        sys.exit(1)

    print("Sample query inserted successfully")

    print("Inserting a sample suppression into the suppression spec table...")
    with open("sample_suppression.qs", "r") as ss:
        suppression_spec = ss.read()

    try:
        ctx.cursor().execute(INSERT_SAMPLE_SUPPRESSION_SPEC_QUERY.format(suppression_spec))
    except Exception as e:
        print("Failed to insert the sample suppression with error {}".format(e))
        sys.exit(1)

    print("Sample suppression inserted successfully")


def test(account):
    print("Testing Snowflake configuration to ensure that account permissions are correct...")

    success = 1
    while success == 1:
        try:
            key_pwd = getpass.getpass("Please enter the password for the private key configured for your SnowAlert user: ")

            with open("rsa_key.p8", "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=key_pwd.encode(),
                    backend=default_backend()
                )
            success = 0
        except Exception as e:
            print("Failed to decrypt the private key with error {}".format(e))
            print("Retrying...")


    pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption())

    print("Authenticating to Snowflake as the SnowAlert user...")
    try:
        context = snowflake.connector.connect(
            user='snowalert',
            account=account,
            private_key=pkb)
    except Exception as e:
        print("Failed to authenticate as the SnowAlert user with error {}".format(e))
        sys.exit(1)

    print("Successfully authenticated as SnowAlert user")

    cs = context.cursor()

    print("Listing the current warehouse...")
    try:
        cs.execute("select current_warehouse();")
        print(cs.fetchall())
    except Exception as e:
        print("Failed to select the current warehouse with error {}".format(e))
        sys.exit(1)

    print("Describing the alerts table...")
    try:
        cs.execute("desc table snowalert.public.alerts;")
        print(cs.fetchall())
    except Exception as e:
        print("Failed to describe the alerts table with error {}".format(e))
        sys.exit(1)

    print("Describing the queries table...")
    try:
        cs.execute("desc table snowalert.public.snowalert_queries;")
        print(cs.fetchall())
    except Exception as e:
        print("Failed to describe the queries table with error {}".format(e))
        sys.exit(1)

    print("Describing the suppressions tables...")
    try:
        cs.execute("desc table snowalert.public.snowalert_queries;")
        print(cs.fetchall())
    except Exception as e:
        print("Failed to describe the suppressions table with error {}".format(e))
        sys.exit(1)

    print("Snowflake successfully configured!")
    return key_pwd

def terraform_init(key_pwd, account, jira_user, jira_password, jira_url, jira_project, jira_flag):
    print("jira flag: {}".format(jira_flag))
    call("terraform init -input=false", shell=True)
    call("terraform plan -input=false -var 'password={}' -var 'jira_password={}' -out=tfplan".format(key_pwd, jira_password), shell=True)
    call("terraform apply -input=false tfplan", shell=True)

def build_packages():
    print("Building packages for lambdas...")
    # We need to change to the directory where the python files are, or we won't actually include them in the zips when we build
    os.chdir("..")
    call ("docker run --rm --mount type=bind,source=\"$(pwd)\",target=/var/task lambci/lambda:build-python3.6 scripts/package-lambda-function.sh all", shell=True)
    # And then change back to the config directory to run terraform
    os.chdir("config")
    # and now we need to move the zips here so that Terraform can see them
    call("cp ../*.zip .", shell=True) 

def jira_integration():
    flag = input("Would you like to integrate Jira with SnowAlert (Y/N)? ")
    if (flag == "Yes" or flag == "y" or flag == "Yes" or flag == "Y"):
        jira_user = input("Please enter the username for the SnowAlert user in Jira: ")
        jira_password = getpass.getpass("Please enter the password for the SnowAlert user in Jira: ")
        jira_url = input("Please enter the URL for the Jira integration: ")
        if jira_url[:8] != "https://":
            jira_url = "https://" + jira_url
        print("Please enter the project tag for the alerts...")
        print("Note that this should be the text that will prepend the ticket id; if the project is SnowAlert")
        print("and the tickets will be SA-XXXX, then you should enter 'SA' for this prompt.")
        jira_project = input("Please enter the project tag for the alerts from SnowAlert: ")
        return 1, jira_user, jira_password, jira_url, jira_project
    else:
        return 0, "placeholder", "", "placeholder", "placeholder"

def write_flag_file(jira_user, jira_project, jira_url, jira_flag, snowflake_account):
    with open("terraform.tfvars", "w") as v:
        v.write('jira_user = "{}"\n'.format(jira_user))
        v.write('jira_project = "{}"\n'.format(jira_project))
        v.write('jira_url = "{}"\n'.format(jira_url))
        v.write('jira_flag = "{}"\n'.format(jira_flag))
        v.write('snowflake_account = "{}"\n'.format(snowflake_account))

def full_test(jira_flag):
    region = input("What region are the SnowAlert lambdas configured in? ")
    print("Invoking the Query Wrapper function...")
    check = call("aws lambda invoke --invocation-type RequestResponse --function-name snowalert-query-wrapper --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
    if check != 0:
        print("The Query Wrapper failed to run. After diagnosing and fixing the error, please run this function again,")
        print("followed by the Suppression Wrapper and then the Jira Integration Alert Handler (if Jira is configured).")
        sys.exit(1)
    print("Invoking the Suppression Wrapper function...")
    check = call("aws lambda invoke --invocation-type RequestResponse --function-name snowalert-suppression-wrapper --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
    if check != 0:
        print("The Suppression Wrapper function failed to run. After diagnosing and fixing the error, please run this function again,")
        print("followed by the Jira Integration Alert Handler if Jira is configured.")
    if jira_flag == 1:
        print("Invoking the Jira Integration function...")
        check = call("aws lambda invoke --invocation-type RequestResponse --function-name snowalert-jira-integration --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
        if check != 0:
            print("The Jira Integration Alert Handler failed to run. After diagnosing and fixing the error, please run this function again.")


if __name__ == '__main__':
    ctx, account = login()
    setup_user(ctx)
    warehouse_setup(ctx)
    pwd = test(account)
    query_setup(ctx)
    jira_flag, jira_user, jira_password, jira_url, jira_project = jira_integration()
    write_flag_file(jira_user, jira_project, jira_url, jira_flag, account)
    # Building the packages takes about two minutes per lambda, which runs about ten minutes if you build all five lambdas.
    # If you want to build the packages yourself, then uncomment the following line in the installer. Otherwise, you can use
    # the prebuilt packages included in the project and deploy them without building.
    #build_packages()
    terraform_init(pwd, account, jira_user, jira_password, jira_url, jira_project, jira_flag)
    print("Installation completed successfully!")
    print("Invoking the lambdas to test the end to end flow...")
    full_test(jira_flag)
    # we had to move some zip files into the config directory to make it easy to upload them, so let's remove those now
    call("rm *.zip", shell=True)
    call("rm outputfile.txt", shell=True)
    print("SnowAlert is now fully deployed in your environment! You can check the alerts table to see the test")
    print("alerts that were created as part of this installation process; if Jira is integrated with your deployment, you ")
    print("should also see the Jira tickets that were automatically created for those alerts. If the tickets weren't created,")
    print("or if there are no alerts in the table, it means something has gone wrong; you can check the Cloudwatch logs for the")
    print("lambda functions to find the error.")

