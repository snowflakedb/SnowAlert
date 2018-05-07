resource "aws_vpc" "main" {
  cidr_block = "10.7.0.0/16"
}

resource "aws_internet_gateway" "igw" {
  vpc_id = "${aws_vpc.main.id}"
}

resource "aws_subnet" "usw2a" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "10.7.0.0/18"
}

resource "aws_subnet" "lambda" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "10.7.64.0/24"
}

resource "aws_route_table_association" "lambda" {
  subnet_id      = "${aws_subnet.lambda.id}"
  route_table_id = "${aws_route_table.nat.id}"
}

resource "aws_route_table" "nat" {
  vpc_id = "${aws_vpc.main.id}"

  tags {
    "Name" = "nat"
  }
}

resource "aws_route" "nat" {
  route_table_id         = "${aws_route_table.nat.id}"
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = "${aws_nat_gateway.lambda.id}"
}

resource "aws_eip" "lambda" {
  vpc = true
}

resource "aws_nat_gateway" "lambda" {
  allocation_id = "${aws_eip.lambda.id}"
  subnet_id     = "${aws_subnet.usw2a.id}"
}

resource "aws_key_pair" "pbennes_20150826" {
  key_name   = "pbennes_20150826"
  public_key = "${file("pbennes_20150826.pub")}"
}

data "aws_ami" "centos6" {
  most_recent      = true
  executable_users = ["all"]

  filter {
    name   = "owner-alias"
    values = ["aws-marketplace"]
  }

  filter {
    name   = "name"
    values = ["CentOS Linux 6 x86_64 HVM EBS *"]
  }

  owners = ["679593333241"] # CentOS official account?
}

resource "aws_iam_role" "pbtest" {
  name = "pbtest"
  path = "/"

  assume_role_policy = <<POLICY
{
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      }
    }
  ],
  "Version": "2012-10-17"
}
POLICY
}

resource "aws_iam_instance_profile" "pbtest" {
  name = "${aws_iam_role.pbtest.name}"
  path = "/"
  role = "${aws_iam_role.pbtest.name}"
}

data "aws_security_group" "default" {
  vpc_id = "${aws_vpc.main.id}"
  name   = "default"
}

resource "aws_security_group_rule" "ingress_vpn" {
  type        = "ingress"
  from_port   = 0
  to_port     = 65535
  protocol    = "all"
  cidr_blocks = ["54.213.100.75/32"]

  security_group_id = "${data.aws_security_group.default.id}"
}

resource "aws_instance" "pbtest" {
  count                       = 3
  subnet_id                   = "${aws_subnet.usw2a.id}"
  ami                         = "${data.aws_ami.centos6.id}"
  instance_type               = "t2.medium"
  associate_public_ip_address = "true"
  key_name                    = "${aws_key_pair.pbennes_20150826.key_name}"
  iam_instance_profile        = "${aws_iam_instance_profile.pbtest.name}"

  vpc_security_group_ids = [
    "${data.aws_security_group.default.id}",
  ]

  tags {
    Name = "pbtest${count.index}"
  }
}

data "aws_route_table" "pbtestigw" {
  vpc_id = "${aws_vpc.main.id}"

  filter {
    name   = "association.main"
    values = ["true"]
  }
}

resource "aws_route" "igw" {
  route_table_id         = "${data.aws_route_table.pbtestigw.route_table_id}"
  gateway_id             = "${aws_internet_gateway.igw.id}"
  destination_cidr_block = "0.0.0.0/0"
}

/*
resource "aws_s3_bucket_notification" "pbtest_snowpipe" {
  bucket = "${aws_s3_bucket.pbtest.id}"

  queue {
    queue_arn = "arn:aws:sqs:us-west-2:494544507972:sf-snowpipe-AIDAJTEARQU2ZK36WBN64-YvEUohqSRWDSojAz4MMXAg"
    events    = ["s3:ObjectCreated:*"]
  }
}
*/

