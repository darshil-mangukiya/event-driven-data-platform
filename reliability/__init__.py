"""Local reliability exercises (failure simulations) for the data platform.

These are **local resilience tests / failure simulations**, not real
production incidents — see docs/reliability.md. Each scenario in
``reliability/scenarios/`` proves something specific and concrete about how
the platform detects and handles a failure mode, using real code paths
wherever the local environment allows (the same validation, deduplication,
watermarking, and reconciliation functions the platform actually runs), and
reports ``not_run``/``simulated`` for the parts that need live
infrastructure (a running Kafka broker, PostgreSQL, Redis, or Docker) not
available in a given environment — see ``reliability/models.py::StepResult``.
"""
