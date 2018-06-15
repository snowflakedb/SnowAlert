#! /bin/bash

aws kms encrypt --key-id $1 --plaintext $2 --output text --query CiphertextBlob > encrypted_password.txt
cat rsa_key.p8 | base64 > rsa_key.b

# A newline gets appended to the end of the files, and we need to trim that off or decryption will fail

cat encrypted_password.txt | tr -d '\n' > encrypted_password
cat rsa_key.b | tr -d '\n' > rsa_key.b64

rm encrypted_password.txt
rm rsa_key.b

if [ ! -z "$3" ]; then
    aws kms encrypt --key-id $1 --plaintext $3 --output text --query CiphertextBlob > encrypted_jira_password.txt
    cat encrypted_jira_password.txt | tr -d '\n' > encrypted_jira_password
    rm encrypted_jira_password.txt
fi
