"""Data product / consumer requirements governance layer.

This package loads and cross-validates the machine-readable contracts in
``contracts/data_products/`` (registry.yml, consumers.yml, requirements.yml)
against the actual implemented system — analytics-service API routes, the
event contract registry, the data catalog, the metric-formula contract, and
the SLO catalog — so a broken reference (a product pointing at an endpoint
that doesn't exist, a requirement pointing at an unknown consumer) is a
caught error, not a silent documentation drift.

See docs/data-products.md and docs/consumer-requirements.md for the
narrative version of what this package validates.
"""
