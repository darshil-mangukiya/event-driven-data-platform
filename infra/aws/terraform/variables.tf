variable "name_prefix" {
  type        = string
  description = "Name prefix for platform resources."
  default     = "cloud-data-platform"
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC ID."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnets for RDS, MSK, Redis, and ECS tasks."
}

variable "db_username" {
  type        = string
  description = "RDS master username."
  default     = "platform"
}

variable "db_password" {
  type        = string
  description = "RDS master password. Use a secret manager in production."
  sensitive   = true
}

variable "allowed_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks allowed to reach internal platform services."
  default     = []
}

