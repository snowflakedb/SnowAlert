{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObjectAcl",
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "${s3_bucket_arn}/security/${data_classification}/${log_type}/${bucket_prefix_name}/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "${s3_bucket_arn}",
      "Condition": {
        "StringLike": {
          "s3:prefix": "security/${data_classification}/${log_type}/${bucket_prefix_name}/*"
        }
      }
    }
  ]
}
