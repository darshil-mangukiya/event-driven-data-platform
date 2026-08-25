# EKS target for the platform's Kubernetes deployment (deploy/kubernetes/,
# deploy/helm/cloudscale/ — locally validated against a real kind cluster,
# see evidence/validation/kubernetes-verification.md and
# evidence/validation/helm-verification.md). Added alongside the
# already-existing ECS cluster in main.tf rather than replacing it — ECS
# remains a valid alternative target this repo already modeled; EKS is
# added because it's the mapping this platform's actual local Kubernetes
# work (kind + Helm) targets in production, not a redundant addition.
#
# Validation covers `terraform fmt` and `terraform validate`; no apply was
# run against AWS; see COST_NOTES.md.

resource "aws_iam_role" "eks_cluster" {
  name = "${var.name_prefix}-eks-cluster"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "platform" {
  name     = "${var.name_prefix}-eks"
  role_arn = aws_iam_role.eks_cluster.arn
  version  = "1.29"

  vpc_config {
    subnet_ids              = var.private_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = false
  }

  encryption_config {
    provider {
      key_arn = aws_kms_key.platform.arn
    }
    resources = ["secrets"]
  }

  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]
}

resource "aws_iam_role" "eks_node_group" {
  name = "${var.name_prefix}-eks-nodes"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_node_worker" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_node_cni" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_node_ecr_readonly" {
  role       = aws_iam_role.eks_node_group.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_eks_node_group" "platform" {
  cluster_name    = aws_eks_cluster.platform.name
  node_group_name = "${var.name_prefix}-platform-nodes"
  node_role_arn   = aws_iam_role.eks_node_group.arn
  subnet_ids      = var.private_subnet_ids
  instance_types  = ["t4g.medium"]

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 4
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_node_worker,
    aws_iam_role_policy_attachment.eks_node_cni,
    aws_iam_role_policy_attachment.eks_node_ecr_readonly,
  ]
}

# One ECR repository per locally-built service image
# (docker/Dockerfile.service SERVICE_PATH targets) — the real set this
# platform actually builds, not an invented list.
resource "aws_ecr_repository" "service_images" {
  for_each = toset([
    "ingestion-service",
    "processing-service",
    "analytics-service",
    "metadata-service",
    "demo-dashboard",
    "ops-console",
    "schema-registry-service",
  ])

  name                 = "${var.name_prefix}/${each.key}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.platform.arn
  }
}
