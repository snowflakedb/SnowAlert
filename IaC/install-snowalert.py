import sys
import os
import getpass
import uuid
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

GRANT_PRIVILEGES_SCHEMA_QUERY = "grant all privileges on all schemas in database snowalert to role SNOWALERT;"
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
    password = getpass.getpass("Please enter the password for the user you provided above. Alternatively, you can press Enter to use SSO for authentication instead: ")
    region = input("Please enter the region where your Snowflake account is located; if the region is us-west-2, or if you don't know your region, press Enter to use the default: ")

    print("Authenticating to Snowflake...")
    # Unfortunately, we've got to use a nested if statement; we can't pass a string to snowflake.connector.connect().
    if password == '':
        if region == '':
            try:
                ctx = snowflake.connector.connect(user=username, account=account, authenticator='externalbrowser')
            except Exception as e:
                print("Snowflake connection failed with error {}".format(e))
                sys.exit(1)
        else:
            try:
                ctx = snowflake.connector.connect(user=username, account=account, authenticator='externalbrowser', region=region)
            except Exception as e:
                print("Snowflake connection failed with error {}".format(e))
                sys.exit(1)
    else:
        if region == '':
            try:
                ctx = snowflake.connector.connect(user=username, account=account, password=password)
            except Exception as e: 
                print("Snowflake connection failed with error {}".format(e))
                sys.exit(1)
        else:
            try:
                ctx = snowflake.connector.connect(user=username, account=account, password=password, region=region)
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
        print("Failed to use the database with error {}".format(e))
        sys.exit(1)
    print("Now using the database created")

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

    success = 1
    while success == 1:
        success = call("./privatekey.sh", shell=True)

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
    print("Verifying that the sample query and suppression haven't already been inserted...")
    try:
        ctx.cursor().execute("delete from snowalert.public.snowalert_queries where query_spec like '%60c99650bb2943cb844fa2cb6d58f448%';")
    except Exception as e:
        print("Failed to clean the query spec table with error {}".format(e))

    try:
        ctx.cursor().execute("delete from snowalert.public.suppression_queries where suppression_spec like '%7ce9eee71fa5403e9d605343148ddd36%';")
    except Exception as e:
        print("Failed to clean the suppression spec table with error {}".format(e))

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
    # We call 'terraform destroy' at the start to clean up any terraform resources from a previous execution of the installer that was aborted.
    # This avoids a bug if Terraform breaks after the KMS key is decrypted, meaning that kms-helper.sh never gets called with the current
    # private key generated by the installer.

    check = call("terraform init -input=false", shell=True)
    if check != 0:
        print("Terraform failed to initialize! Please examine the error and re-run the installer when the problem is resolved.")
        sys.exit(1)

    print("Deleting any existing terraform-managed resources to ensure the installer is working from a clean slate...")

    # We run `terraform destroy` twice here; there's a weird error where Terraform will occasionally fail to find an IAM policy the first time destory runs;
    # running it a second time invariably resolves the error. If destroy fails twice in a row, then something unexpected is wrong and we should abort.

    check = call("terraform destroy -input=false -var 'password={}' -var 'jira_password={}' -auto-approve".format(key_pwd, jira_password), shell=True)
    if check != 0:
        print("Terraform had an error while attempting to destroy a resource! This can happen sometimes; the installer will try again. If Terraform continues to fail,")
        print("the installer will abort. If it continues to run, then everything should be fine.")
        check = call("terraform destroy -input=false -var 'password={}' -var 'jira_password={}' -auto-approve".format(key_pwd, jira_password), shell=True)
    if check != 0:
        print("Terraform was unable to destroy the resources it created from a previous installation! Please examine the error and re-run the installer when the problem is resolved.")
    
    check = call("terraform plan -input=false -var 'password={}' -var 'jira_password={}' -out=tfplan".format(key_pwd, jira_password), shell=True)
    if check != 0:
        print("Terraform failed to plan! Please examine the error and re-run the installer when the problem is resolved.")
        sys.exit(1)

    check = call("terraform apply -input=false tfplan", shell=True)
    if check != 0:
        print("Terraform failed to create all of the resources it needs! Please examine the error and re-run the installer when the problem is resolved.")
        sys.exit(1)

def build_packages():
    print("Building packages for lambdas...")
    # We need to change to the directory where the python files are, or we won't actually include them in the zips when we build
    os.chdir("..")
    call ("./scripts/package-lambda-function.sh all", shell=True)
    # And then change back to the IaC directory to run terraform
    os.chdir("IaC")
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
    print("You will now be prompted to name several AWS resources: an AWS S3 bucket, and the AWS Lambda functions which will execute the work of SnowAlert.")
    print("The AWS S3 bucket must have a globally unique name. If it turns out the name you select is not globally unique, you can change it without running")
    print("the installer again by modifying the value in terraform.tfvars.")
    print("")
    print("The Lambda functions have default names, which you can opt to use by pressing Enter at the prompt for each lambda.")
    print("")
    print("The S3 bucket can also be given a default name, which will be 'snowalert-deploy-' followed by a random GUID.")

    s3_bucket_name = input("S3 Bucket Name (suggestion: <company>-SnowAlert-Deploy. Press Enter for SnowAlert-Deploy-<randomstring>): ")
    if s3_bucket_name == '':
        s3_bucket_name = 'snowalert-deploy-' + uuid.uuid4().hex
    s3_bucket_name = s3_bucket_name.lower()

    query_runner_name = input("Query Runner Function name (press Enter for default name 'query_runner'): ")
    if query_runner_name == '':
        query_runner_name = "query_runner"
    query_runner_name = query_runner_name.lower()

    query_wrapper_name = input("Query Wrapper Function name (press Enter for default name 'query_wrapper'): ")
    if query_wrapper_name == '':
        query_wrapper_name = "query_wrapper"
    query_wrapper_name = query_wrapper_name.lower()

    suppression_runner_name = input("Suppression Runner Function name (press Enter for default name 'suppression_runner'): ")
    if suppression_runner_name == '':
        suppression_runner_name  = 'suppression_runner'
    suppression_runner_name = suppression_runner_name.lower()

    suppression_wrapper_name = input("Suppression Wrapper Function name (press Enter for default name 'suppression_wrapper'): ")
    if suppression_wrapper_name == '':
        suppression_wrapper_name = 'suppression_wrapper'
    suppression_wrapper_name = suppression_wrapper_name.lower()

    if jira_flag == 1:
        jira_integration_name = input("Jira Integration Function name (press Enter for default name 'jira_integration'): ")
        if jira_integration_name == '':
            jira_integration_name = "jira_integration"
    else:
        jira_integration_name = "placeholder"

    with open("terraform.tfvars", "w") as v:
        v.write('jira_user = "{}"\n'.format(jira_user))
        v.write('jira_project = "{}"\n'.format(jira_project))
        v.write('jira_url = "{}"\n'.format(jira_url))
        v.write('jira_flag = "{}"\n'.format(jira_flag))
        v.write('snowflake_account = "{}"\n'.format(snowflake_account))
        v.write('s3_bucket_name = "{}"\n'.format(s3_bucket_name))
        v.write('query_runner_name = "{}"\n'.format(query_runner_name))
        v.write('query_wrapper_name = "{}"\n'.format(query_wrapper_name))
        v.write('suppression_runner_name = "{}"\n'.format(suppression_runner_name))
        v.write('suppression_wrapper_name = "{}"\n'.format(suppression_wrapper_name))
        v.write('jira_integration_name = "{}"\n'.format(jira_integration_name))

    return query_wrapper_name, suppression_wrapper_name, jira_integration_name

def full_test(jira_flag, query_wrapper_name, suppression_wrapper_name, alert_handler_name):
    region = input("What region are the SnowAlert lambdas configured in? ")
    print("Invoking the Query Wrapper function...")
    check = call("aws lambda invoke --invocation-type RequestResponse --function-name "+query_wrapper_name+" --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
    if check != 0:
        print("The Query Wrapper failed to run. After diagnosing and fixing the error, please run this function again,")
        print("followed by the Suppression Wrapper and then the Jira Integration Alert Handler (if Jira is configured).")
        sys.exit(1)
    print("Invoking the Suppression Wrapper function...")
    check = call("aws lambda invoke --invocation-type RequestResponse --function-name "+suppression_wrapper_name+" --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
    if check != 0:
        print("The Suppression Wrapper function failed to run. After diagnosing and fixing the error, please run this function again,")
        print("followed by the Jira Integration Alert Handler if Jira is configured.")
    if jira_flag == 1:
        print("Invoking the Jira Integration function...")
        check = call("aws lambda invoke --invocation-type RequestResponse --function-name "+jira_integration_name+" --region "+region+" --log-type Tail --payload '{}' outputfile.txt", shell=True)
        if check != 0:
            print("The Jira Integration Alert Handler failed to run. After diagnosing and fixing the error, please run this function again.")


if __name__ == '__main__':
    ctx, account = login()
    setup_user(ctx)
    warehouse_setup(ctx)
    pwd = test(account)
    query_setup(ctx)
    jira_flag, jira_user, jira_password, jira_url, jira_project = jira_integration()
    query_wrapper_name, suppression_wrapper_name, jira_integration_name = write_flag_file(jira_user, jira_project, jira_url, jira_flag, account)
    # Building the packages takes about two minutes per lambda, which runs about ten minutes if you build all five lambdas.
    # If you want to build the packages yourself, then answer 'y' here! Otherwise, you can use
    # the prebuilt packages included in the project and deploy them without building.
    build_flag = input("Do you want to build the packages from scratch? This will take between eight and ten minutes. (Y/N): ")
    if build_flag == 'y' or build_flag == 'Y':
        build_packages()
    terraform_init(pwd, account, jira_user, jira_password, jira_url, jira_project, jira_flag)
    print("Installation completed successfully!")
    print("Invoking the lambdas to test the end to end flow...")
    full_test(jira_flag, query_wrapper_name, suppression_wrapper_name, jira_integration_name)
    call("rm outputfile.txt", shell=True)
    print("SnowAlert is now fully deployed in your environment! You can check the alerts table to see the test")
    print("alerts that were created as part of this installation process; if Jira is integrated with your deployment, you ")
    print("should also see the Jira tickets that were automatically created for those alerts. If the tickets weren't created,")
    print("or if there are no alerts in the table, it means something has gone wrong; you can check the Cloudwatch logs for the")
    print("Lambda functions to find the error.")
    sys.exit(0)

