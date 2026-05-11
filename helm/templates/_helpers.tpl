{{/*
Expand the name of the chart.
*/}}
{{- define "kubentic-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "kubentic-operator.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "kubentic-operator.name" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Namespace helper.
*/}}
{{- define "kubentic-operator.namespace" -}}
{{- .Values.namespace.name }}
{{- end }}

{{/*
Access token secret name.
*/}}
{{- define "kubentic-operator.tokenSecretName" -}}
{{- if .Values.agent.accessToken.existingSecret -}}
{{- .Values.agent.accessToken.existingSecret -}}
{{- else -}}
kubentic-token
{{- end -}}
{{- end }}

{{/*
Access token secret key.
*/}}
{{- define "kubentic-operator.tokenSecretKey" -}}
{{- .Values.agent.accessToken.existingKey | default "token" -}}
{{- end }}

{{/*
VictoriaMetrics URL — uses bundled vmstack service if not overridden.
Service name is fixed via fullnameOverride: "vmstack" in the sub-chart values.
*/}}
{{- define "kubentic-operator.vmURL" -}}
{{- if .Values.agent.collection.metrics.victoriaMetricsURL -}}
{{- .Values.agent.collection.metrics.victoriaMetricsURL -}}
{{- else -}}
http://vmsingle-vmstack.{{ include "kubentic-operator.namespace" . }}.svc:8428
{{- end -}}
{{- end }}

{{/*
VictoriaLogs URL — uses bundled vlogs service if not overridden.
Service name is fixed via fullnameOverride: "vlogs" in the sub-chart values.
*/}}
{{- define "kubentic-operator.vlURL" -}}
{{- if .Values.agent.collection.logs.victoriaLogsURL -}}
{{- .Values.agent.collection.logs.victoriaLogsURL -}}
{{- else -}}
http://vlogs-server.{{ include "kubentic-operator.namespace" . }}.svc:9428
{{- end -}}
{{- end }}
