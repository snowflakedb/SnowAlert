resource "aws_kms_key" "prod" {
  description = "key used to encrypt passwords for snowalert to use"
  policy = jsonencode(
    {
      Id = "key-default-1"
      Statement = [
        {
          Action = "kms:*"
          Effect = "Allow"
          Principal = {
            AWS = "arn:aws:iam::${local.account_id}:root"
          }
          Resource = "*"
          Sid      = "Enable IAM User Permissions"
        },
      ]
      Version = "2012-10-17"
    }
  )
  tags = {}
}

resource "aws_iam_policy" "kms_decrypt" {
  name        = "snowalert_kms_decrypt"
  path        = "/"
  description = ""
  policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Action   = "kms:Decrypt"
          Effect   = "Allow"
          Resource = aws_kms_key.prod.arn
          Sid      = ""
        },
      ]
    }
  )
}
