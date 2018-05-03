#!/bin/bash

# blindly clean up from previous executions if they exist
rm -rf lambda_function
rm -f lambda_function.zip
# create a virtual environnment named "lambda_function"
virtualenv -p python3 lambda_function
# activate the virtual environnment
source lambda_function/bin/activate
# install/upgrade pip
pip install --upgrade pip
# install/upgrade snowflake-connector-python
pip install --upgrade snowflake-connector-python
# add our python lambda handler code to the lambda zip archive
zip -9 lambda_function.zip lambda_function.py
# hack to make the snowflake namespace work in the lambda python environnment
touch $VIRTUAL_ENV/lib/python3.6/site-packages/snowflake/__init__.py
# add all the contents of site-packages to the zip archive
DIR=`pwd`
cd $VIRTUAL_ENV/lib/python3.6/site-packages
zip -r9 $DIR/lambda_function.zip .
cd $VIRTUAL_ENV/lib64/python3.6/site-packages
zip -r9 $DIR/lambda_function.zip .
