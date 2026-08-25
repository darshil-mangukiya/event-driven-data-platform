# ADR 0008: `kind` for Local Kubernetes Verification, EKS as the Documented AWS Target

## Status

Accepted

## Context

The platform needed real Kubernetes execution evidence (beyond YAML
that has never been applied) without provisioning paid cloud
infrastructure autonomously, which the operating constraints for this
work explicitly forbid without authorization.

## Decision

Use `kind` (Kubernetes-in-Docker) for actual local `kubectl apply`
execution of the raw manifests and the Helm chart, and keep the AWS
target (EKS + IAM + ECR, `infra/aws/terraform/eks.tf`) at
`terraform fmt`/`validate` only — never `terraform apply`. KEDA is
installed and its `ScaledObject` applied against the same `kind` cluster
for the same reason: real operator behavior, no paid infrastructure.

## Consequences

Kubernetes claims in this project are backed by an actual `kubectl get
pods` / `helm lint && helm template` trace against a real (if local)
control plane, not aspirational YAML. The known limitation is that
`kind`'s CNI does not reliably support a pod reaching its own workload
through its own ClusterIP Service in every configuration — this
surfaced as Kafka never reaching `Ready` in this cluster, and is reported
as a diagnosed environment limitation
(`kubernetes-verification.md`) rather than worked around by weakening the
readiness probe or claiming success. The EKS Terraform stays
validate-only; provisioning it for a truly complete live cluster
end-to-end run would require explicit user authorization and real AWS
spend, neither of which verification had.
