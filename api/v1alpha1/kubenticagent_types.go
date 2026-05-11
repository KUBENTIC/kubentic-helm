package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ka
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.schedule"
// +kubebuilder:printcolumn:name="Last Scan",type="date",JSONPath=".status.lastScanTime"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type KubenticAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KubenticAgentSpec   `json:"spec,omitempty"`
	Status KubenticAgentStatus `json:"status,omitempty"`
}

type KubenticAgentSpec struct {
	// AccessToken references the Secret containing the Kubentic access token.
	// +kubebuilder:validation:Required
	AccessToken AccessTokenRef `json:"accessToken"`

	// APIKey optionally references a Secret key that is injected into agent
	// pods as the API_KEY environment variable.  The backend server creates or
	// updates this Secret so every CronJob run receives the current key.
	APIKey *corev1.SecretKeySelector `json:"apiKey,omitempty"`

	// Schedule is the cron expression for the collection job (e.g. "0 * * * *").
	// +kubebuilder:default="0 * * * *"
	Schedule string `json:"schedule,omitempty"`

	// Collection configures what data to collect from the cluster.
	Collection CollectionSpec `json:"collection,omitempty"`

	// Backend configures the Kubentic backend endpoint.
	Backend BackendSpec `json:"backend,omitempty"`

	// AgentOverride allows customizing the agent pod template.
	AgentOverride *AgentOverrideSpec `json:"agentOverride,omitempty"`

	// CreateRbac controls whether the operator creates ServiceAccount + ClusterRole + ClusterRoleBinding.
	// Set to false if you provide your own serviceAccountName with the required permissions.
	// +kubebuilder:default=true
	CreateRbac bool `json:"createRbac,omitempty"`

	// ServiceAccountName is the name of an existing ServiceAccount to use.
	// Only relevant when createRbac is false.
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Suspend pauses collection when true. The CronJob remains but jobs are not triggered.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// ConcurrencyPolicy specifies how concurrent collection runs are handled.
	// +kubebuilder:default=Forbid
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`

	// SuccessfulJobsHistoryLimit is the number of successful jobs to retain in history.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit is the number of failed jobs to retain in history.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`

	// ActiveDeadlineSeconds specifies the maximum duration in seconds a job pod may run.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// BackoffLimit specifies the number of retries before marking the job failed.
	// +kubebuilder:default=2
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`
}

type AccessTokenRef struct {
	// SecretRef is the reference to the Secret and key containing the access token.
	// +kubebuilder:validation:Required
	SecretRef corev1.SecretKeySelector `json:"secretRef"`
}

type CollectionSpec struct {
	// Logs configures pod log collection via the Kubernetes API.
	Logs LogCollectionSpec `json:"logs,omitempty"`

	// Metrics configures metrics collection from a VictoriaMetrics or Prometheus instance.
	Metrics MetricsCollectionSpec `json:"metrics,omitempty"`
}

type LogCollectionSpec struct {
	// Enabled controls whether log collection runs. Default true.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// SinceHours is how many hours back to collect logs from each pod.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=168
	SinceHours int32 `json:"sinceHours,omitempty"`

	// MaxBytesPerPod limits log bytes collected per pod container. 0 = unlimited.
	// +kubebuilder:default=0
	MaxBytesPerPod int64 `json:"maxBytesPerPod,omitempty"`

	// MaxTailLines is the max number of log lines collected per container.
	// +kubebuilder:default=30000
	// +kubebuilder:validation:Minimum=100
	MaxTailLines int32 `json:"maxTailLines,omitempty"`

	// Concurrency is the number of parallel pod log fetches.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	Concurrency int32 `json:"concurrency,omitempty"`

	// VictoriaLogsURL is the base URL of VictoriaLogs for fetching full restart history.
	// If empty, historical run collection and ghost pod detection are skipped.
	VictoriaLogsURL string `json:"victoriaLogsURL,omitempty"`

	// VLHistoryDays is how many days back to query VictoriaLogs for restart history.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=90
	VLHistoryDays int32 `json:"vlHistoryDays,omitempty"`

	// NamespaceSelector selects namespaces to collect logs from.
	// Nil means all namespaces.
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// ExcludeNamespaces is a list of namespace names to skip.
	ExcludeNamespaces []string `json:"excludeNamespaces,omitempty"`
}

type MetricsCollectionSpec struct {
	// Enabled controls whether metrics collection runs.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// VictoriaMetricsURL is the base URL of the VictoriaMetrics (or Prometheus) instance.
	// Example: "http://victoria-metrics-single-server.monitoring.svc:8428"
	VictoriaMetricsURL string `json:"victoriaMetricsURL,omitempty"`

	// RangeSeconds is the time window for range queries. Default matches SinceHours * 3600.
	// +kubebuilder:default=10800
	// +kubebuilder:validation:Minimum=60
	RangeSeconds int32 `json:"rangeSeconds,omitempty"`

	// Step is the resolution step for range queries (e.g. "60s", "5m").
	// +kubebuilder:default="60s"
	Step string `json:"step,omitempty"`
}

type BackendSpec struct {
	// URL is the Kubentic backend ingest endpoint.
	// +kubebuilder:default="https://pa.kubentic.ai:8443"
	URL string `json:"url,omitempty"`

	// TLSSkipVerify disables TLS certificate verification (useful for self-signed certs in dev).
	// +kubebuilder:default=false
	TLSSkipVerify bool `json:"tlsSkipVerify,omitempty"`

	// UploadMaxRetries is the number of upload retry attempts with exponential backoff.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	UploadMaxRetries int32 `json:"uploadMaxRetries,omitempty"`
}

type AgentOverrideSpec struct {
	// Image overrides the default agent container image.
	Image *ImageSpec `json:"image,omitempty"`

	// Resources sets CPU/memory requests and limits for the agent container.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Env are additional environment variables injected into the agent container.
	Env []corev1.EnvVar `json:"env,omitempty"`

	// Affinity sets pod scheduling constraints.
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations sets pod scheduling tolerations.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector sets node selection labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Annotations are added to the job pod template.
	Annotations map[string]string `json:"annotations,omitempty"`
}

type ImageSpec struct {
	// Name is the full container image reference including tag or digest.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// PullPolicy controls when the image is pulled.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// PullSecrets are the names of Secrets in the same namespace to use for pulling the image.
	PullSecrets []corev1.LocalObjectReference `json:"pullSecrets,omitempty"`
}

// KubenticAgentStatus defines the observed state.
type KubenticAgentStatus struct {
	// Conditions represent the latest available observations of the agent's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is the high-level summary of the agent's current state.
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Suspended
	Phase string `json:"phase,omitempty"`

	// LastScanTime is the timestamp of the last successful data upload.
	LastScanTime *metav1.Time `json:"lastScanTime,omitempty"`

	// NextScanTime is the scheduled time of the next collection run.
	NextScanTime *metav1.Time `json:"nextScanTime,omitempty"`

	// AgentVersion is the container image tag of the currently deployed agent.
	AgentVersion string `json:"agentVersion,omitempty"`

	// CronJobName is the name of the managed CronJob resource.
	CronJobName string `json:"cronJobName,omitempty"`

	// ObservedGeneration is the most recent spec generation that the controller has processed.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// Condition type constants.
const (
	ConditionTypeReady     = "Ready"
	ConditionTypeDegraded  = "Degraded"
	ConditionTypeSuspended = "Suspended"

	PhaseRunning   = "Running"
	PhasePending   = "Pending"
	PhaseSuspended = "Suspended"
	PhaseFailed    = "Failed"
	PhaseSucceeded = "Succeeded"
)

// +kubebuilder:object:root=true
type KubenticAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubenticAgent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KubenticAgent{}, &KubenticAgentList{})
}
