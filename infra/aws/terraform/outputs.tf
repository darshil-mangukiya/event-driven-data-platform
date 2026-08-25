output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "postgres_endpoint" {
  value = aws_db_instance.postgres.address
}

output "redis_primary_endpoint" {
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "msk_bootstrap_brokers_tls" {
  value = aws_msk_cluster.kafka.bootstrap_brokers_tls
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.platform.name
}

