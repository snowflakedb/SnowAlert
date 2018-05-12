#!/bin/bash

if [ $# == 0 ];
    then echo 'This is a build script for python files you want to deploy in AWS Lambda'
    echo 'The first argument should be the .py with the entry point of the lambda function'
    echo 'You can pass in other required .py files as arguments as well'
    echo 'The script zips recursively, so you can also pass in a directory if you like'
    echo 'The script should output $1.zip, where $1 is the first argument.'
    echo 'You can then upload the zip to Lambda either through the console or via S3'
    echo 'If you want to `pip install jira`, pass in jira as the final argument'
    exit
fi

# blindly clean up from previous executions if they exist
rm -f  *.zip

# create a virtual environnment named "lambda_function"
virtualenv -p python3 lambda_function

# activate the virtual environnment
source lambda_function/bin/activate

# install/upgrade pip
pip install --upgrade pip

# install/upgrade snowflake-connector-python
pip install --upgrade snowflake-connector-python

# optionally install/upgrade jira
last=${!#}
if [ $last == jira ];
    then pip install --upgrade jira
fi

# add our python lambda handler code to the lambda zip archive

v=$1
v2=${v%.*}
zip -r9 $v2.zip $@
# hack to make the snowflake namespace work in the lambda python environnment
touch $VIRTUAL_ENV/lib/python3.6/site-packages/snowflake/__init__.py
# add all the contents of site-packages to the zip archive
DIR=`pwd`
cd $VIRTUAL_ENV/lib/python3.6/site-packages
zip -r9 $DIR/$v2.zip .
cd $VIRTUAL_ENV/lib64/python3.6/site-packages
zip -r9 $DIR/$v2.zip .

rm -rm lambda_function
