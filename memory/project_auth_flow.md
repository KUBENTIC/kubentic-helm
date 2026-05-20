---
name: project-auth-flow
description: Full subscription validation and Helm install auth flow for Kubentic operator
metadata: 
  node_type: memory
  type: project
  originSessionId: e3488f8f-c7b9-4a44-af19-255ff3b1578d
---

## Architecture: How User Authentication Works at Install Time

**Why:** Only authenticated/subscribed users should be able to install and use the Kubentic operator.

### Flow

1. Backend generates a JWT token for the user (at signup/login on kubentic.ai)
2. User runs `helm install` with `--set agent.accessToken.value=<JWT>`
3. Helm pre-install hook Job fires (subscription-check.yaml) — calls validate endpoint as **query param** (NOT Bearer header)
4. Backend returns `{"api_key": "..."}` in response body
5. Hook stores `api_key` in K8s secret `kubentic-apikey` (key: `api-key`) via direct K8s REST API
6. If 200 → install proceeds. If non-200 → helm install aborts
7. Agent CronJob reads `api_key` from `kubentic-apikey` secret and sends it as a **multipart form field** to `/pa/call`

### CRITICAL: Exact API contract (confirmed by testing)

| Endpoint | Method | Auth mechanism | Notes |
|---|---|---|---|
| `GET /api/v1/auth/validate-foresight` | GET | `?jwt_token=TOKEN` query param | NOT a Bearer header — backend returns 422 if header used |
| `POST /pa/call` | POST | `api_key` as multipart form field | NOT a Bearer header — alongside the `file` field |

### Key Decision: Images are PUBLIC

`ghcr.io/kubentic/operator`, `ghcr.io/kubentic/agent`, `ghcr.io/kubentic/subscription-check` are public.
- Real enforcement is at the API level (`/pa/call` requires valid api_key on every agent run)
- Private images add complexity with minimal security gain

### Three Backend Endpoints

| Endpoint | Host | Purpose | Called by |
|---|---|---|---|
| `GET /api/v1/auth/validate-foresight?jwt_token=TOKEN` | `api.kubentic.ai` | validate JWT at install time | Helm pre-install hook |
| `POST /pa/call` (multipart: api_key + file) | `pa.kubentic.ai:8443` | receive collected data | Agent CronJob (every hour) |

### Two Secrets in Cluster

| Secret | Contains | Used by |
|---|---|---|
| (none — JWT passed directly as helm value) | JWT | Hook only, at install time |
| `kubentic-apikey` (key: `api-key`) | `api_key` from backend response | Agent CronJob on every run |
