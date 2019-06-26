resource "aws_security_group" "snowalert" {
  name        = "${var.name}-snowalert"
  description = "Security group for ${var.name} Snowalert"
  vpc_id      = "${var.vpc_id}"

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
