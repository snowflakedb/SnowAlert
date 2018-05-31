#!/bin/bash

if [[ $# != 3 ]]; then
	printf "Usage: $0 <zip file name> <lambda function name> [<aws profile name>]\nLeave AWS profile name empty for default profile"
	exit
fi

if [ -z "$LAMBDA_DEPLOYMENT_BUCKET" ]; then
	printf "You must set the LAMBDA_DEPLOYMENT_BUCKET environment variable to the name of the S3 bucket\nexport LAMBDA_DEPLOYMENT_BUCKET=<bucket name here>"
	exit
fi

file=$1
lambda=$2
profile=$3

if [ -z "$profile" ]
then
	profile="default"
fi

if [ ! -f $file ]; then
	echo "File $file doesn't exist!"
	exit
elif [[ $file != *.zip ]]; then
	echo "File $file is not a ZIP!"
	exit
else
	aws s3 cp $file s3://$LAMBDA_DEPLOYMENT_BUCKET/$file --profile $profile
	echo "File $file uploaded"
fi

read -p "Enter n to skip updating Lambda function: " choice

if [ "$choice" = "n" ]; then
	echo "Skipping Lambda function update!"
	exit
else
	aws lambda update-function-code --function-name $lambda --s3-bucket $LAMBDA_DEPLOYMENT_BUCKET --s3-key $file --profile $profile
	echo "Lambda function $lambda updated"
fi