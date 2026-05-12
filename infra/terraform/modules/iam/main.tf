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

resource "aws_iam_role" "airflow" {
  name               = "${var.name_prefix}-airflow"
  description        = "Airflow worker role: submits EMR jobs and reads/writes data buckets."
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid    = "DataBucketsRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
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
    sid    = "DataBucketsWrite"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [for arn in var.data_bucket_arns : "${arn}/*"]
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
    sid    = "EMRSubmitAndMonitor"
    effect = "Allow"
    actions = [
      "elasticmapreduce:RunJobFlow",
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:ListSteps",
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:TerminateJobFlows",
    ]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = length(var.passable_role_arns) > 0 ? [1] : []
    content {
      sid       = "PassEMRRoles"
      effect    = "Allow"
      actions   = ["iam:PassRole"]
      resources = var.passable_role_arns
    }
  }
}

resource "aws_iam_role_policy" "airflow" {
  name   = "${var.name_prefix}-airflow"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.airflow.json
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${var.name_prefix}-airflow"
  role = aws_iam_role.airflow.name
  tags = var.tags
}

resource "aws_iam_role" "fastapi" {
  name               = "${var.name_prefix}-fastapi"
  description        = "FastAPI service role: read-only access to curated data."
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "fastapi" {
  statement {
    sid    = "DataBucketsRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
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

  dynamic "statement" {
    for_each = length(var.log_group_arns) > 0 ? [1] : []
    content {
      sid    = "CloudWatchLogs"
      effect = "Allow"
      actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ]
      resources = [for arn in var.log_group_arns : "${arn}:*"]
    }
  }
}

resource "aws_iam_role_policy" "fastapi" {
  name   = "${var.name_prefix}-fastapi"
  role   = aws_iam_role.fastapi.id
  policy = data.aws_iam_policy_document.fastapi.json
}

resource "aws_iam_instance_profile" "fastapi" {
  name = "${var.name_prefix}-fastapi"
  role = aws_iam_role.fastapi.name
  tags = var.tags
}
