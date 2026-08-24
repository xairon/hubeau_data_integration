# Sandbox deployment (proof of concept)

Deploying the warehouse to the research sandbox server through Portainer + GitOps, loading
data from scratch (no migration: `full_bootstrap`).

| | |
|---|---|
| Target | `gpu2.recherche.sandbox.univ-tours.fr` (`10.108.44.30`) — standalone Docker, Portainer agent |
| Compose | [`docker-compose.sandbox.yml`](../docker-compose.sandbox.yml) |
| Environment | [`.env.sandbox.example`](../.env.sandbox.example) |
| Data | Bootstrap from scratch (Hub'Eau + ERA5) |
| CI/CD | Portainer GitOps, auto-redeploy on push to `main` |

> The shared Portainer forbids **both bind mounts and `build:`** inside a stack ("forbidden
> properties"). This deployment uses neither: the custom images are **pre-built by GitLab CI**
> and pushed to the **GitLab Container Registry**, and the compose only references them by tag.
>
> Other differences from production: named volumes (no `init_volumes.sh`), no GPU,
> `postgres_tuning` removed, lower `mem_limit`.
>
> In Portainer, **do not enable "relative path volumes"** — there are no bind mounts.

## Before you start

1. **Outbound network access** to the external APIs — this blocks the bootstrap entirely if
   missing. A university sandbox is often behind a proxy or firewall. Test from the server:
   ```bash
   curl -sI https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations | head -1
   curl -sI https://cds.climate.copernicus.eu | head -1
   ```
2. **Ports `49500`–`49505` free** on `10.108.44.30`.
3. **A valid Copernicus CDS key.**

## 1. Build and push the images (GitLab CI → Container Registry)

1. **Enable the registry**: GitLab → *Settings → General → Visibility* → make sure
   **Container Registry** is on. The path shown under *Deploy → Container Registry* is
   `CI_REGISTRY_IMAGE`, e.g. `registry.scm.univ-tours.fr/ringuet/hubeau_data_integration`.
   Note it — it is the `REGISTRY_PREFIX` of step 3.
2. **Run the pipeline**: a push to `main` triggers the `build` stage of `.gitlab-ci.yml`,
   which builds and pushes three images with kaniko:
   - `hubeau-worker:sandbox`
   - `hubeau-orchestrator:sandbox`
   - `hubeau-postgres-sandbox:sandbox` (TimescaleDB with `init.sql` baked in)

   Follow it in GitLab → *Build → Pipelines*. `build_worker` is the long one (~2 GB, GDAL + dbt).
3. **Check** that *Deploy → Container Registry* lists all three tagged `sandbox`.

## 2. Register the registry in Portainer

The registry is private, so Portainer needs credentials to pull from it.

1. GitLab → *Settings → Repository → Deploy tokens* → create a token scoped
   **`read_registry`**; note the generated username and token.
2. Portainer → *Registries* → **Add registry** → **Custom registry**:
   - URL: the registry **host** (e.g. `registry.scm.univ-tours.fr`)
   - Username / password: the deploy token from step 1.

## 3. Create the stack (compose from Git)

Portainer → *Stacks* → **Add stack** → method **Repository**:

| Field | Value |
|-------|-------|
| Name | `hubeau-sandbox` |
| Repository URL | the GitLab repository URL |
| Repository reference | `refs/heads/main` |
| Compose path | `docker-compose.sandbox.yml` |
| Authentication | a GitLab token if the repository is private |
| Relative path volumes | **unchecked** |

Under **Environment variables** (Advanced mode → paste `.env.sandbox`), at minimum:
`REGISTRY_PREFIX` (from step 1), `DAGSTER_PG_PASSWORD`, `PG_PASSWORD`, `POSTGIS_PASSWORD`,
`COPERNICUS_API_KEY`.

Click **Deploy the stack**. Portainer pulls the images — no build — and the stack is up in a
minute or two.

## 4. Auto rebuild and redeploy

Every push: CI rebuilds the images (the `sandbox` tag is overwritten), then Portainer re-pulls
and redeploys. Two ways to trigger the redeploy.

**Option A — polling (simplest).** Stack → **GitOps updates** → *Enable* → **Polling** `5m`,
and tick **Re-pull image**. Without that tick Portainer keeps the old image behind the same
tag.

**Option B — webhook (driven by CI, after the build).**

1. Stack → **GitOps updates** → *Enable* → **Webhook** → copy the URL.
2. Add a job to `.gitlab-ci.yml`, in a stage that runs *after* the build:
   ```yaml
   deploy_sandbox:
     stage: deploy
     image: curlimages/curl:latest
     script:
       - curl -fsS -X POST "$PORTAINER_WEBHOOK_URL"
     rules:
       - if: '$CI_COMMIT_BRANCH == "main"'
   ```
3. GitLab → *Settings → CI/CD → Variables* → add `PORTAINER_WEBHOOK_URL` (masked).

## 5. Run the initial bootstrap

Once the stack is healthy:

1. Dagster UI: `http://10.108.44.30:49500`
2. Check that the `hubeau_pipeline` code location is loaded (otherwise *Reload*).
3. *Jobs* → **`full_bootstrap`** → **Launchpad** → *Launch Run*.

What gets loaded is driven by `BOOTSTRAP_PARTITIONS`. Unlike the main `docker-compose.yml`,
the sandbox compose **does** forward `BOOTSTRAP_PARTITIONS` and `BOOTSTRAP_CONTINUE_ON_ERROR`
to the worker (`docker-compose.sandbox.yml:106-107`), so setting them in the stack
environment works as written. `BOOTSTRAP_FORCE_RERUN` is *not* forwarded.

**For a proof of concept, restrict it to two or three recent years.** A full ERA5 reload
(1990 → present) takes hours to days and will exhaust the CDS quota.

The job is restartable: state is persisted in `ops.bootstrap_state`, so after a partial
failure (an API 503, say) re-launching resumes where it stopped, especially with
`BOOTSTRAP_CONTINUE_ON_ERROR=true`.

## 6. After a code change

The code is **baked into the worker image** — there is no volume hot reload here.

1. Push → CI rebuilds `hubeau-worker:sandbox`.
2. Portainer re-pulls and redeploys (step 4).
3. Dagster UI → **Reload definitions** on the `hubeau_pipeline` code location (or the
   `reloadRepositoryLocation` GraphQL mutation).

## Sandbox caveats

- **Shared server**: the `495xx` ports are exposed on the sandbox IP. Do not put sensitive
  data here, and change the default passwords.
- **Persistence**: the volumes are Portainer-managed, not external. Removing the stack with
  "remove volumes" wipes the database — acceptable for a POC, since it can be re-bootstrapped.
- **GPU**: unused by this stack (the reservation was removed). The GPU server brings nothing
  here beyond CPU and RAM.
