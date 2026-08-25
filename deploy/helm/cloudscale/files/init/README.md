These are a **synced copy** of the executable SQL behind `database/init/` — Helm's `.Files.Glob`
can only read files inside the chart's own directory tree, so they can't
be referenced in place. `database/init/*.sql` remains the single source of
truth; re-sync before packaging/installing a new chart version:

```bash
make helm-sync-init-scripts
```

The target copies initialization files 001-003 and packages the authoritative
`database/security/tenant_rls.sql` as `004_tenant_rls.sql`. It deliberately does
not copy `database/init/004_apply_tenant_rls.sql`, because that Docker-specific
file uses a `psql` include path mounted only by Compose. This is the same
"copy at build time, document why" pattern this repo already uses for
`docker/Dockerfile.service`'s `COPY contracts /app/contracts`.
