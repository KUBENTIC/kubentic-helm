---
name: project-user-install-commands
description: Exact Helm commands given to end users to install the Kubentic operator
metadata: 
  node_type: memory
  type: project
  originSessionId: e3488f8f-c7b9-4a44-af19-255ff3b1578d
---

## User-Facing Install Commands

**Why:** These are the three commands shown to users on the kubentic.ai dashboard/docs after they get their token.

```bash
helm repo add kubentic https://helm.kubentic.ai
helm repo update
helm install kubentic kubentic/kubentic-operator \
  --namespace kubentic-foresight \
  --create-namespace \
  --set agent.accessToken.value=<YOUR_TOKEN>
```

Token is the only required value. Everything else has defaults.

### Optional overrides users can set

```bash
--set agent.schedule="0 */6 * * *"                  # change collection frequency
--set agent.accessToken.existingSecret=my-secret     # use existing K8s secret
--set agent.collection.logs.enabled=false            # disable log collection
--set victoria-metrics-k8s-stack.enabled=false       # skip bundled VM stack
--set victoria-logs-single.enabled=false             # skip bundled VL stack
```

### What installs

- Kubentic operator (Deployment)
- KubenticAgent CronJob (runs every hour, collects logs + metrics, uploads to pa.kubentic.ai:8443)
- Grafana
- VictoriaMetrics k8s stack (node-exporter, kube-state-metrics, vmagent)
- VictoriaLogs
- Vector log shipper (DaemonSet)

**How to apply:** Use these exact commands when writing docs, onboarding flows, or dashboard copy.
