data "aws_iam_policy_document" "service_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  name               = "${var.name_prefix}-emr-service"
  description        = "EMR service role - cluster lifecycle operations."
  assume_role_policy = data.aws_iam_policy_document.service_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "service_managed" {
  role       = aws_iam_role.service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  name               = "${var.name_prefix}-emr-ec2"
  description        = "EMR EC2 instance profile - data + log access from cluster nodes."
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "ec2_data" {
  statement {
    sid    = "DataBucketsReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
    ]
    resources = [for arn in var.data_bucket_arns : "${arn}/*"]
  }

  statement {
    sid    = "DataBucketsList"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = var.data_bucket_arns
  }

  statement {
    sid    = "LogBucketWrite"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      var.log_bucket_arn,
      "${var.log_bucket_arn}/*",
    ]
  }

  dynamic "statement" {
    for_each = length(var.log_group_arns) > 0 ? [1] : []
    content {
      sid    = "CloudWatchLogs"
      effect = "Allow"
      actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
      ]
      resources = [for arn in var.log_group_arns : "${arn}:*"]
    }
  }

  statement {
    sid    = "EC2DescribeForCoordination"
    effect = "Allow"
    actions = [
      "ec2:DescribeInstances",
      "ec2:DescribeTags",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ec2_data" {
  name   = "${var.name_prefix}-emr-ec2-data"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_data.json
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-emr-ec2"
  role = aws_iam_role.ec2.name
  tags = var.tags
}

data "aws_iam_policy_document" "autoscaling_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = [
        "elasticmapreduce.amazonaws.com",
        "application-autoscaling.amazonaws.com",
      ]
    }
  }
}

resource "aws_iam_role" "autoscaling" {
  name               = "${var.name_prefix}-emr-autoscaling"
  description        = "EMR managed-scaling role."
  assume_role_policy = data.aws_iam_policy_document.autoscaling_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "autoscaling_managed" {
  role       = aws_iam_role.autoscaling.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"
}
