#!/bin/bash

if [ $# == 0 ];
    then echo 'This is the installation script for SnowAlert'
    echo 'It takes two arguments: the file you want to build, and the aws profile being used.'
    echo "If you want to build all files, then provide 'all' instead of a filename"
    echo "Examples: ./install-snowalert.sh alert_handler.py <aws_profile>"
    echo "Examples: ./install-snowalert.sh all <aws_profile>"
    exit
fi

file=$1
profile=$2

docker run --rm --mount type=bind,source="$(pwd)",target=/var/task lambci/lambda:build-python3.6 scripts/package-lambda-function.sh $file

if [ $1 == "all" ]; then
    /bin/bash ./scripts/upload-lambda-function.sh alert_handler.zip snowalert-jira-integration $profile
    /bin/bash ./scripts/upload-lambda-function.sh query_runner.zip snowalert-query-executor $profile
    /bin/bash ./scripts/upload-lambda-function.sh query_wrapper.zip snowalert-query-wrapper $profile
    /bin/bash ./scripts/upload-lambda-function.sh suppression_runner.zip snowalert-suppression-executor $profile
    /bin/bash ./scripts/upload-lambda-function.sh suppression_wrapper.zip snowalert-suppression-wrapper $profile
else
    read -p "What lamda are you deploying to? " lambda
    name=${file%.*}

    /bin/bash ./scripts/upload-lambda-function.sh $name.zip $lambda $profile
fi
