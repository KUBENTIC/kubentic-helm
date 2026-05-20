---
name: project-versions
description: Helm chart version history and what each release fixed — use to understand current state
metadata: 
  node_type: memory
  type: project
  originSessionId: 2343a781-49fd-4440-b45c-0e49c8f9924b
---

## Version History

| Version | Key fix |
|---|---|
| v0.1.0 | Initial release |
| v0.1.1–v0.1.4 | Early fixes (pre-session) |
| v0.1.5 | Last version before this session |
| v0.1.6 | Fixed: replaced bitnami/kubectl with curlimages/curl; kubectl→K8s REST API for secret; hook-failed delete policy; removed ttlSecondsAfterFinished |
| v0.1.7 | Fixed: validate endpoint uses `?jwt_token=TOKEN` query param not Bearer header |
| v0.1.8 | Fixed: `/pa/call` sends `api_key` as multipart form field not Bearer header; agent image rebuilt |
| v0.1.9 | Fixed: multi-arch images (amd64+arm64); removed hardcoded GOARCH=amd64 from Dockerfile; added QEMU+Buildx to workflow |
| v0.2.0 | Fixed: subscription-check image moved from Docker Hub (curlimages/curl) to ghcr.io/kubentic/subscription-check to avoid rate limits on Azure/GCP; added subscription-check/Dockerfile |

**Current version: v0.2.0**

## Images (all on ghcr.io, multi-arch amd64+arm64, public)

| Image | Purpose |
|---|---|
| `ghcr.io/kubentic/operator:latest` | Kubernetes operator binary |
| `ghcr.io/kubentic/agent:latest` | Data collection agent (Python) |
| `ghcr.io/kubentic/subscription-check:latest` | Alpine+curl for pre-install hook |

## Chart Repository

- Helm repo: `https://helm.kubentic.ai/`
- Add: `helm repo add kubentic https://helm.kubentic.ai/`
- Published via GitHub Actions on `v*` tag push to `develop` branch
- Chart and images both publish from the same tag

**Why:** project-versions decays fast — update when tagging a new release.
