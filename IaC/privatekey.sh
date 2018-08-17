#! /bin/bash

// -passout pass:$1 here lets us pass in the password on the command line, instead of having openssl prompt us for the password.

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -passout pass:$1 -out rsa_key.p8

if [ $? -eq 1 ];
then
	echo "Failed to generate the private key. Retrying..."
	exit 1
fi

echo "Now generating the public key..."

// -passin pass:$1 does the same for generating the public key.

openssl rsa -in rsa_key.p8 -passin pass:$1 -pubout -out rsa_key.pub

if [ $? -eq 1 ];
then
	echo "Failed to generate the public key. Starting over..."
	exit 1
fi
