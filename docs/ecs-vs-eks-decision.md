# ECS vs EKS Decision

## Recommendation

Start with ECS/Fargate for the platform MVP unless the company already standardizes on Kubernetes.

## ECS Advantages

- lower operational overhead
- simpler service deployment model
- strong fit for FastAPI services
- easier for a small platform team

## EKS Advantages

- richer ecosystem for custom operators
- standard Kubernetes portability
- better fit when Spark, Kafka operators, and service mesh are already in use

## Decision Rule

Use ECS/Fargate for the API and worker services when speed and simplicity matter most. Move to EKS when the organization has Kubernetes expertise and needs cluster-level extensibility.
