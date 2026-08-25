# Cloud Deployment Plan

The AWS skeleton in `infra/aws/terraform` maps local components to managed cloud services.

## AWS Mapping

| Local Component | AWS Target |
| --- | --- |
| Kafka | Amazon MSK |
| PostgreSQL | Amazon RDS PostgreSQL |
| Redis | Amazon ElastiCache Redis |
| MinIO | Amazon S3 |
| FastAPI services | ECS Fargate or EKS |
| Spark jobs | EMR Serverless, AWS Glue, or Spark Operator on EKS |
| Prometheus/Grafana | Amazon Managed Prometheus and Grafana |

## Deployment Sequence

1. Provision VPC networking and private subnets.
2. Provision S3 lakehouse bucket, RDS, ElastiCache, and MSK.
3. Run Alembic migrations against RDS.
4. Build and push service images.
5. Deploy services to ECS or EKS with secrets and health checks.
6. Create Kafka topics with production replication and retention.
7. Schedule Spark jobs and data quality checks.
8. Enable observability dashboards and alerts.

The Terraform files are a deployment skeleton, not a claim that the project was deployed to a real AWS account.

