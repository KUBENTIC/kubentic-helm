---
name: project-subscription-enforcement
description: "How subscription enforcement works at install time and runtime, including cancellation behaviour"
metadata: 
  node_type: memory
  type: project
  originSessionId: e3488f8f-c7b9-4a44-af19-255ff3b1578d
---

## Subscription Enforcement Layers

**Why:** Understanding where and how access is blocked prevents gaps in security.

### Layer 1: Helm pre-install hook (install time)
- `subscription-check.yaml` runs as a pre-install/pre-upgrade Job
- Calls `GET https://api.kubentic.ai/api/v1/auth/validate-foresight` with Bearer JWT
- 200 → helm install continues
- non-200 → helm install aborts with error message
- Can be bypassed with `--set subscriptionCheck.enabled=false` — acceptable, see Layer 2

### Layer 2: API enforcement (runtime, every hour)
- Agent calls `POST https://pa.kubentic.ai:8443/pa/call` on every CronJob run
- Backend checks JWT validity + subscription_active on every request
- If subscription cancelled → 403 → data never reaches backend → product is dead
- This is the real enforcement layer — cannot be bypassed

### Cancellation Behaviour
- Cancel subscription in DB → `/pa/call` returns 403 immediately
- Currently running pods keep running (cached images) but uploads fail
- Product is functionally dead from cancellation moment

### Why Images are Public (not private)
- Discussed and decided: private images add complexity, not meaningful security
- Even with images, agent is useless without valid JWT for `/pa/call`
- See [[project-auth-flow]] for full reasoning

**How to apply:** When someone asks "what if subscription expires" or "can they bypass the check" — Layer 2 is always the answer.
