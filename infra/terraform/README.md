# DataFabrik Terraform

Reusable Terraform modules for the DataFabrik AWS infrastructure.

## Layout

```
modules/
  s3/           Data lake + log buckets (encryption, versioning, public access block, lifecycle)
  vpc/          VPC, public/private subnets, NAT, S3 gateway endpoint, EMR security groups
  iam/          Application service roles (Airflow, FastAPI) with least-privilege policies
  emr/          EMR service role, EC2 instance profile, autoscaling role
  cloudwatch/   Log groups with configurable retention

environments/
  dev/          Composition that wires the five modules together for dev
```

## Quickstart

```bash
cd environments/dev
terraform init                          # download providers, link modules (run once)
terraform plan -out=tfplan              # show what will change, save the plan
terraform apply tfplan                  # apply exactly the saved plan (no surprises)
terraform destroy                       # tear it all down when finished
```

The `-out=tfplan` / `apply tfplan` two-step is the safer pattern: `apply` runs the exact plan you reviewed, not a fresh one computed at apply time.

State is local by default (`terraform.tfstate` in the env directory, gitignored). Switch to an S3 backend by adding `backend.tf` once a state bucket exists.

### Common commands

```bash
terraform fmt -recursive                # format all .tf files in place
terraform validate                      # static-check the config (no AWS calls)
terraform plan                          # show drift / pending changes
terraform plan -out=tfplan              # save the plan to apply later
terraform apply tfplan                  # apply a saved plan
terraform apply -auto-approve           # skip the confirmation prompt (be careful)
terraform destroy                       # delete every resource in state
terraform output                        # print all outputs
terraform output -raw vpc_id            # print one output, no quoting
terraform state list                    # list managed resources
terraform state show <addr>             # inspect one resource
```

Run all of these from `environments/dev/` (or another env directory).

## What gets created in dev

- 1 VPC (`10.20.0.0/16`) with 2 public + 2 private subnets across 2 AZs, 1 NAT gateway, 1 S3 gateway endpoint
- 3 empty EMR security groups (EMR populates ingress rules at cluster launch)
- 4 S3 buckets: `<project>-<env>-{raw,staging,curated,logs}-<account-id>` with AES-256 SSE, versioning, public access blocked
- 3 CloudWatch log groups: `/<project>-<env>/{airflow,fastapi,emr}` with 14-day retention
- 4 IAM roles + instance profiles: `airflow`, `fastapi`, `emr-service`, `emr-ec2`, plus `emr-autoscaling`

No EMR cluster, EC2 instance, or RDS database is created — those come in later tickets and consume the IAM + networking outputs from here.

## Least-privilege policy notes

- **EMR EC2 instance role** does **not** use the legacy `AmazonElasticMapReduceforEC2Role` (which grants broad S3, DynamoDB, and EC2 permissions). Instead it uses a custom inline policy scoped to the data + log bucket ARNs.
- **Airflow role** has `s3:GetObject` / `s3:PutObject` only on configured bucket ARNs, can submit/monitor EMR jobs, and can `iam:PassRole` only to the specific EMR role ARNs (not `*`).
- **FastAPI role** is read-only on the data buckets — no write actions, no EMR permissions.
- **S3 buckets** have `BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets` all set.

## Cost (us-east-1, idle dev environment)

| Resource             | Approx. monthly cost |
| -------------------- | -------------------- |
| NAT gateway          | ~$32 + data transfer |
| S3 buckets (empty)   | $0                   |
| CloudWatch log groups (empty) | $0          |
| IAM roles            | $0                   |
| VPC, subnets, SGs    | $0                   |
| **Baseline idle**    | **~$32/mo**          |

Set `enable_nat_gateway = false` in `terraform.tfvars` to drop to ~$0/mo at the cost of private-subnet internet egress (EMR clusters in private subnets need NAT or VPC endpoints for AWS APIs).

Run `terraform destroy` to remove everything.

## Inputs

See [environments/dev/variables.tf](environments/dev/variables.tf). Common overrides:

```hcl
# terraform.tfvars
region             = "us-west-2"
vpc_cidr           = "10.30.0.0/16"
enable_nat_gateway = false
log_retention_days = 7
```
