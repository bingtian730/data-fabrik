resource "aws_s3_bucket" "this" {
  for_each = var.buckets

  bucket        = "${var.name_prefix}-${each.key}-${var.name_suffix}"
  force_destroy = each.value.force_destroy
  tags          = merge(var.tags, { Name = "${var.name_prefix}-${each.key}" })
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id
  versioning_configuration {
    status = each.value.versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = var.buckets

  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = {
    for k, v in var.buckets : k => v
    if v.expire_noncurrent_after_days != null
  }

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = each.value.expire_noncurrent_after_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}
