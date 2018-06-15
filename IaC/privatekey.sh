#! /bin/bash

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

if [ $? -eq 1 ];
then
	echo "Failed to generate the private key. Retrying..."
	exit 1
fi

echo "Now generating the public key..."

openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

if [ $? -eq 1 ];
then
	echo "Failed to generate the public key. Starting over..."
	exit 1
fi
