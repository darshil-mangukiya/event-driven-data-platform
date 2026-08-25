# AWS Cost Notes

The Terraform in this folder is an architecture skeleton, not a full production billing model.

Production cost work should be handled before apply:

- Confirm target region and availability requirements.
- Price Kafka, Postgres, Redis, Spark/Glue/EMR, observability, NAT, and data transfer separately.
- Decide whether this platform starts on ECS/Fargate or EKS.
- Set explicit retention for Kafka topics, logs, metrics, and object-storage zones.
- Add budget alerts for daily spend, Kafka broker cost, RDS storage growth, and observability ingestion.
- Tag all resources with `project`, `environment`, `owner`, `cost_center`, and `tenant_scope`.

See `docs/cost-model.md` for the planning model.
