data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"
  account_id  = data.aws_caller_identity.current.account_id
}

module "vpc" {
  source = "../../modules/vpc"

  name_prefix        = local.name_prefix
  cidr_block         = var.vpc_cidr
  az_count           = 2
  enable_nat_gateway = var.enable_nat_gateway
}

module "s3" {
  source = "../../modules/s3"

  name_prefix = local.name_prefix
  name_suffix = local.account_id

  buckets = {
    raw = {
      versioning                   = true
      expire_noncurrent_after_days = 30
    }
    staging = {
      versioning                   = true
      expire_noncurrent_after_days = 14
    }
    curated = {
      versioning                   = true
      expire_noncurrent_after_days = 90
    }
    logs = {
      versioning                   = false
      expire_noncurrent_after_days = 30
    }
  }
}

module "cloudwatch" {
  source = "../../modules/cloudwatch"

  name_prefix = local.name_prefix
  log_groups = {
    airflow = { retention_days = var.log_retention_days }
    fastapi = { retention_days = var.log_retention_days }
    emr     = { retention_days = var.log_retention_days }
  }
}

module "emr" {
  source = "../../modules/emr"

  name_prefix = local.name_prefix
  data_bucket_arns = [
    module.s3.bucket_arns["raw"],
    module.s3.bucket_arns["staging"],
    module.s3.bucket_arns["curated"],
  ]
  log_bucket_arn = module.s3.bucket_arns["logs"]
  log_group_arns = [module.cloudwatch.log_group_arns["emr"]]
}

module "iam" {
  source = "../../modules/iam"

  name_prefix = local.name_prefix
  data_bucket_arns = [
    module.s3.bucket_arns["raw"],
    module.s3.bucket_arns["staging"],
    module.s3.bucket_arns["curated"],
  ]
  log_group_arns = [
    module.cloudwatch.log_group_arns["airflow"],
    module.cloudwatch.log_group_arns["fastapi"],
  ]
  passable_role_arns = [
    module.emr.service_role_arn,
    module.emr.ec2_role_arn,
    module.emr.autoscaling_role_arn,
  ]
}
