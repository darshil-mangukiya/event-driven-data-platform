# Incident Copilot Control Boundaries

The incident copilot analyzes existing evidence and returns text for human
review.

## Supported behavior

- summarize incident evidence;
- classify incident type and severity;
- rank probable causes with confidence values and evidence references;
- identify affected tenants or services present in the evidence;
- select a runbook identifier from the static catalog;
- recommend review steps as text.

## Excluded behavior

The package cannot delete or replay Kafka data, restart services, scale
Kubernetes resources, modify PostgreSQL or IAM policies, run Terraform, deploy
code, or write to a database.

The package imports no command runner, infrastructure client, or database write
path. `IncidentAnalysis.requires_human_approval` is constrained to `True` by
the Pydantic schema.

## Evidence grounding

`IncidentAnalysis.evidence_ids_referenced` lists the evidence items used by the
analysis. The default provider builds this list from the supplied
`EvidenceBundle`. When the bundle cannot support a conclusion, it returns
`insufficient_evidence=True`.
