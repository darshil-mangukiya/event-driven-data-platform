# AWS Deployment Skeleton

This Terraform-style skeleton shows how the platform maps to AWS. It is intentionally a practical starting point rather than a claimed deployed environment.

Target mapping:

- Kafka: Amazon MSK.
- PostgreSQL: Amazon RDS PostgreSQL.
- Redis: Amazon ElastiCache Redis.
- Object storage: Amazon S3.
- Microservices: ECS Fargate or EKS.
- Spark: EMR Serverless, Glue, or EKS Spark Operator.
- Observability: CloudWatch, Amazon Managed Prometheus, Grafana.

Typical flow:

```bash
terraform init
terraform plan \
  -var='vpc_id=vpc-...' \
  -var='private_subnet_ids=["subnet-a","subnet-b","subnet-c"]'
```

This folder avoids hard-coded account IDs, secrets, or real production identifiers.

