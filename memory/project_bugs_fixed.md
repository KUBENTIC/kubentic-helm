---
name: project-bugs-fixed
description: "Bugs found and fixed during helm install testing, with root causes and solutions"
metadata: 
  node_type: memory
  type: project
  originSessionId: e3488f8f-c7b9-4a44-af19-255ff3b1578d
---

## Bug 1: namespace.yaml as pre-install hook caused Terminating race condition

**Symptom:** `serviceaccounts "kubentic-hook" is forbidden: unable to create new content in namespace kubentic-foresight because it is being terminated`

**Root cause:** `namespace.yaml` had pre-install hook annotations. When install fails, Helm deletes all hook resources including the namespace. Next install fires while namespace is still Terminating.

**Fix:** Removed hook annotations from `namespace.yaml` — now a regular chart template. Use `--create-namespace`.

---

## Bug 2: ttlSecondsAfterFinished race with Helm hook watcher

**Symptom:** `resource not ready, name: kubentic-subscription-check, kind: Job, status: NotFound, context deadline exceeded`

**Root cause:** `ttlSecondsAfterFinished: 120` on the hook Job caused K8s TTL controller to delete the Job before Helm's watcher could observe its final state (success or failure). Helm saw NotFound.

**Fix:** Removed `ttlSecondsAfterFinished`. Added `hook-failed` to `hook-delete-policy` on both the Job and RBAC resources so Helm manages cleanup. Also added `activeDeadlineSeconds: 120` for a hard cap.

---

## Bug 3: bitnami/kubectl image too large (300MB) → 5-10 min timeout

**Symptom:** `status: Failed, context deadline exceeded` after 5-10 minutes

**Root cause:** `bitnami/kubectl:latest` is ~300MB. On fresh nodes it took longer than Helm's 5-minute timeout to pull. Also didn't need kubectl at all.

**Fix:** 
- Replaced with `ghcr.io/kubentic/subscription-check:latest` (our own tiny image: alpine + curl, ~8MB)
- Replaced `kubectl create secret` with direct K8s REST API calls using the pod's mounted service account token
- RBAC verbs changed from `["create","update","patch","apply"]` to `["create","patch"]` (`apply` is not a valid K8s verb)

---

## Bug 4: validate endpoint uses query param not Bearer header

**Symptom:** `HTTP 422 — Field required: query.jwt_token`

**Root cause:** Script was sending `Authorization: Bearer TOKEN`. Backend actually expects `?jwt_token=TOKEN` as a URL query parameter.

**Fix:** Changed curl call from `-H "Authorization: Bearer ..."` to `"URL?jwt_token=${KUBENTIC_ACCESS_TOKEN}"` in `subscription-check.yaml`.

---

## Bug 5: /pa/call expects api_key as multipart form field not Bearer header

**Symptom:** Agent pod CrashLoopBackOff — upload failed every run

**Root cause:** `collect.py` was sending `Authorization: Bearer {api_key}` header. Backend's `/pa/call` endpoint is `multipart/form-data` with two required fields: `api_key` (string) and `file` (zip). Confirmed via Swagger UI screenshot.

**Fix:** In `collect.py` upload function:
```python
# Before (wrong)
form.add_field("file", fh, ...)
session.post(ENDPOINT, data=form, headers={"Authorization": f"Bearer {KUBENTIC_TOKEN}"})

# After (correct)
form.add_field("api_key", KUBENTIC_TOKEN)
form.add_field("file", fh, ...)
session.post(ENDPOINT, data=form)
```

---

## Bug 6: Namespace stuck in Terminating due to KubenticAgent finalizer

**Symptom:** `kubectl delete namespace kubentic-foresight` hangs; namespace stays in Terminating

**Root cause:** KubenticAgent CR has `kubentic.io/finalizer` finalizer. When operator is already uninstalled, no controller processes the finalizer so namespace can never finish deleting.

**Fix:**
```bash
kubectl patch kubenticagent kubentic-agent \
  -n kubentic-foresight \
  --type=json \
  -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

---

## Bug 7: GOARCH=amd64 hardcoded in Dockerfile → exec format error on Azure/GCP

**Symptom:** `exec /manager: exec format error` — operator CrashLoopBackOff on Azure/GCP, works on AWS

**Root cause:** `Dockerfile` had `RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ...` hardcoded. Image pushed as amd64-only. If a local ARM Mac previously pushed `latest`, it would be arm64, breaking amd64 nodes.

**Fix:** Updated `Dockerfile` to use Buildx `TARGETARCH`/`TARGETOS` args:
```dockerfile
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build ...
```
Added QEMU + Buildx setup to `build-images.yml` with `platforms: linux/amd64,linux/arm64`.

---

## Bug 8: Docker Hub rate limits break subscription-check on Azure/GCP

**Symptom:** `status: Failed, context deadline exceeded` on Azure/GCP but not AWS

**Root cause:** `curlimages/curl:8.11.1` is on Docker Hub. Azure/GCP nodes hit rate limits or get slow pulls from Docker Hub. AWS was already cached.

**Fix:** Created `subscription-check/Dockerfile` (alpine:3.20 + curl) built and hosted at `ghcr.io/kubentic/subscription-check:latest`. All three images now on GHCR, zero Docker Hub dependency.

---

## Namespace stuck in Terminating — emergency fix (generic)

```bash
# Patch out finalizer from the blocking CR
kubectl patch kubenticagent kubentic-agent \
  -n kubentic-foresight \
  --type=json \
  -p='[{"op":"remove","path":"/metadata/finalizers"}]'

# Nuclear option if above doesn't work
kubectl get namespace kubentic-foresight -o json | \
  python3 -c "import sys,json; ns=json.load(sys.stdin); ns['spec']['finalizers']=[]; print(json.dumps(ns))" | \
  kubectl replace --raw "/api/v1/namespaces/kubentic-foresight/finalize" -f -
```
