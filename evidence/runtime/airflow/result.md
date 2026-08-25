# Airflow Runtime Result

Status: **EXECUTED AND VERIFIED** for both DAGs.

Airflow imported two DAGs with no import errors. The repository models 22 tasks
across them. The first operational run failed because subprocesses did not
receive the project lineage module on `PYTHONPATH`; the shared command wrapper
was fixed and regression-tested. The complete 12-task operational validation
DAG then succeeded for logical date 2026-09-03.

The 10-task batch DAG was then run with the local `LocalExecutor`. Its first
attempt exposed a real portability defect: lakehouse compaction relied on the
job's `s3a://` defaults even though the Airflow Spark image does not include the
S3A connector. The DAG now supplies the existing local filesystem paths and
runs compaction after event normalization. A second attempt found no normalized
rows in the default seven-day window; the successful bounded run used the
supported `CLOUDSCALE_SPARK_WINDOW_DAYS=365` override for the retained synthetic
dataset.

Final batch run:

- command: `CLOUDSCALE_SPARK_WINDOW_DAYS=365 airflow dags test cloudscale_batch_jobs 2026-09-03T17:43:00+00:00`
- run ID: `manual__2026-09-03T17:43:00+00:00`
- result: 10 succeeded, 0 failed, 0 retries
- duration: 34.467046 seconds

Kafka and Spark remain the execution engines; Airflow provides bounded local
orchestration evidence rather than production scheduling evidence.
