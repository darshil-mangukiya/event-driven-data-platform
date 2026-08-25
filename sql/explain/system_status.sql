explain (analyze false, buffers false, costs true)
select distinct on (service_name) service_name, status, latency_ms, error_count,
       throughput_per_minute, kafka_lag, cache_hit_rate, event_timestamp
from service_health_metrics
where tenant_id = 'tenant_demo'
order by service_name, event_timestamp desc;

