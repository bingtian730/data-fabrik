resource "aws_cloudwatch_log_group" "this" {
  for_each = var.log_groups

  name              = "/${var.name_prefix}/${each.key}"
  retention_in_days = each.value.retention_days

  tags = merge(var.tags, { Name = "${var.name_prefix}-${each.key}" })
}
