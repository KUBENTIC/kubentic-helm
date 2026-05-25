package controller

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubenticv1alpha1 "github.com/kubentic/operator/api/v1alpha1"
)

// ─── Naming helpers ───────────────────────────────────────────────────────────

func agentSAName(agent *kubenticv1alpha1.KubenticAgent) string {
	return fmt.Sprintf("kubentic-agent-%s", agent.Name)
}

func agentClusterRoleName(agent *kubenticv1alpha1.KubenticAgent) string {
	return fmt.Sprintf("kubentic-agent-%s-%s", agent.Namespace, agent.Name)
}

func agentCRBName(agent *kubenticv1alpha1.KubenticAgent) string {
	return fmt.Sprintf("kubentic-agent-%s-%s", agent.Namespace, agent.Name)
}

func agentCronJobName(agent *kubenticv1alpha1.KubenticAgent) string {
	return fmt.Sprintf("kubentic-agent-%s", agent.Name)
}

func agentImageTag(agent *kubenticv1alpha1.KubenticAgent) string {
	img := resolveImage(agent)
	parts := strings.SplitN(img, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return "latest"
}

func resolveImage(agent *kubenticv1alpha1.KubenticAgent) string {
	if agent.Spec.AgentOverride != nil && agent.Spec.AgentOverride.Image != nil {
		return agent.Spec.AgentOverride.Image.Name
	}
	return defaultAgentImage
}

func resolveImagePullPolicy(agent *kubenticv1alpha1.KubenticAgent) corev1.PullPolicy {
	if agent.Spec.AgentOverride != nil && agent.Spec.AgentOverride.Image != nil {
		if agent.Spec.AgentOverride.Image.PullPolicy != "" {
			return agent.Spec.AgentOverride.Image.PullPolicy
		}
	}
	return corev1.PullIfNotPresent
}

func resolveSchedule(agent *kubenticv1alpha1.KubenticAgent) string {
	if agent.Spec.Schedule != "" {
		return agent.Spec.Schedule
	}
	return defaultSchedule
}

func resolveBackendURL(agent *kubenticv1alpha1.KubenticAgent) string {
	if agent.Spec.Backend.URL != "" {
		return agent.Spec.Backend.URL
	}
	return defaultBackendURL
}

func resolveSAName(agent *kubenticv1alpha1.KubenticAgent) string {
	if !agent.Spec.CreateRbac && agent.Spec.ServiceAccountName != "" {
		return agent.Spec.ServiceAccountName
	}
	return agentSAName(agent)
}

func commonLabels(agent *kubenticv1alpha1.KubenticAgent) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "kubentic-agent",
		"app.kubernetes.io/instance":   agent.Name,
		"app.kubernetes.io/managed-by": "kubentic-operator",
		"kubentic.io/agent":            agent.Name,
	}
}

// ─── ServiceAccount ──────────────────────────────────────────────────────────

func buildServiceAccount(agent *kubenticv1alpha1.KubenticAgent) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agentSAName(agent),
			Namespace: agent.Namespace,
			Labels:    commonLabels(agent),
		},
	}
}

// ─── ClusterRole ─────────────────────────────────────────────────────────────

func buildClusterRole(agent *kubenticv1alpha1.KubenticAgent) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   agentClusterRoleName(agent),
			Labels: commonLabels(agent),
		},
		Rules: []rbacv1.PolicyRule{
			{
				// Read pods and their logs across the entire cluster.
				APIGroups: []string{""},
				Resources: []string{"namespaces", "pods", "pods/log"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				// Read workload resources for inventory.
				APIGroups: []string{"apps"},
				Resources: []string{"deployments", "daemonsets", "statefulsets", "replicasets"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				// Read node info.
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

// ─── ClusterRoleBinding ──────────────────────────────────────────────────────

func buildClusterRoleBinding(agent *kubenticv1alpha1.KubenticAgent) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:   agentCRBName(agent),
			Labels: commonLabels(agent),
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     agentClusterRoleName(agent),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      agentSAName(agent),
				Namespace: agent.Namespace,
			},
		},
	}
}

// ─── CronJob ─────────────────────────────────────────────────────────────────

func buildCronJob(agent *kubenticv1alpha1.KubenticAgent) *batchv1.CronJob {
	successLimit := int32(3)
	if agent.Spec.SuccessfulJobsHistoryLimit != nil {
		successLimit = *agent.Spec.SuccessfulJobsHistoryLimit
	}
	failedLimit := int32(3)
	if agent.Spec.FailedJobsHistoryLimit != nil {
		failedLimit = *agent.Spec.FailedJobsHistoryLimit
	}
	backoffLimit := int32(0)
	if agent.Spec.BackoffLimit != nil {
		backoffLimit = *agent.Spec.BackoffLimit
	}

	// activeDeadline caps the total time a collection Job may stay active
	// (including time its pod spends Pending). Without it, an unschedulable
	// or hung pod keeps the Job "active" forever, and with ConcurrencyPolicy
	// Forbid that blocks every future scheduled run.
	activeDeadline := int64(1800)
	if agent.Spec.ActiveDeadlineSeconds != nil {
		activeDeadline = *agent.Spec.ActiveDeadlineSeconds
	}

	concurrencyPolicy := batchv1.ForbidConcurrent
	switch agent.Spec.ConcurrencyPolicy {
	case "Allow":
		concurrencyPolicy = batchv1.AllowConcurrent
	case "Replace":
		concurrencyPolicy = batchv1.ReplaceConcurrent
	}

	podAnnotations := map[string]string{}
	if agent.Spec.AgentOverride != nil && agent.Spec.AgentOverride.Annotations != nil {
		podAnnotations = agent.Spec.AgentOverride.Annotations
	}

	jobSpec := batchv1.JobSpec{
		BackoffLimit:          &backoffLimit,
		ActiveDeadlineSeconds: &activeDeadline,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      commonLabels(agent),
				Annotations: podAnnotations,
			},
			Spec: buildPodSpec(agent),
		},
	}

	suspend := agent.Spec.Suspend
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agentCronJobName(agent),
			Namespace: agent.Namespace,
			Labels:    commonLabels(agent),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   resolveSchedule(agent),
			ConcurrencyPolicy:          concurrencyPolicy,
			Suspend:                    &suspend,
			SuccessfulJobsHistoryLimit: &successLimit,
			FailedJobsHistoryLimit:     &failedLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: jobSpec,
			},
		},
	}
}

func buildPodSpec(agent *kubenticv1alpha1.KubenticAgent) corev1.PodSpec {
	container := buildContainer(agent)

	spec := corev1.PodSpec{
		ServiceAccountName: resolveSAName(agent),
		// Never (paired with BackoffLimit=0) gives deterministic single-attempt
		// semantics: a failed collection is not restarted in place by the kubelet,
		// so a hung upload is not re-run and the bundle is not re-sent. The next
		// attempt happens on the next schedule instead.
		RestartPolicy: corev1.RestartPolicyNever,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
			RunAsUser:    int64Ptr(1000),
			FSGroup:      int64Ptr(1000),
		},
		Containers: []corev1.Container{container},
	}

	if agent.Spec.AgentOverride != nil {
		spec.Affinity = agent.Spec.AgentOverride.Affinity
		spec.Tolerations = agent.Spec.AgentOverride.Tolerations
		spec.NodeSelector = agent.Spec.AgentOverride.NodeSelector

		if agent.Spec.AgentOverride.Image != nil {
			spec.ImagePullSecrets = agent.Spec.AgentOverride.Image.PullSecrets
		}
	}

	return spec
}

func buildContainer(agent *kubenticv1alpha1.KubenticAgent) corev1.Container {
	tlsSkipVerify := "false"
	if agent.Spec.Backend.TLSSkipVerify {
		tlsSkipVerify = "true"
	}

	// uploadRetries := int32(3)
	// if agent.Spec.Backend.UploadMaxRetries > 0 {
	// 	uploadRetries = agent.Spec.Backend.UploadMaxRetries
	// }

	logsEnabled := "true"
	if !agent.Spec.Collection.Logs.Enabled {
		logsEnabled = "false"
	}
	sinceHours := agent.Spec.Collection.Logs.SinceHours
	if sinceHours == 0 {
		sinceHours = 3
	}
	concurrency := agent.Spec.Collection.Logs.Concurrency
	if concurrency == 0 {
		concurrency = 10
	}

	metricsEnabled := "false"
	if agent.Spec.Collection.Metrics.Enabled {
		metricsEnabled = "true"
	}
	rangeSeconds := agent.Spec.Collection.Metrics.RangeSeconds
	if rangeSeconds == 0 {
		rangeSeconds = int32(sinceHours) * 3600
	}

	maxTailLines := agent.Spec.Collection.Logs.MaxTailLines
	if maxTailLines == 0 {
		maxTailLines = 30000
	}
	vlHistoryDays := agent.Spec.Collection.Logs.VLHistoryDays
	if vlHistoryDays == 0 {
		vlHistoryDays = 7
	}
	step := agent.Spec.Collection.Metrics.Step
	if step == "" {
		step = "60s"
	}

	env := []corev1.EnvVar{
		{
			Name: "KUBENTIC_ACCESS_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &agent.Spec.AccessToken.SecretRef,
			},
		},
		{Name: "KUBENTIC_BACKEND_URL", Value: resolveBackendURL(agent)},
		{Name: "TLS_SKIP_VERIFY", Value: tlsSkipVerify},
		// {Name: "UPLOAD_MAX_RETRIES", Value: fmt.Sprintf("%d", uploadRetries)},
		{Name: "COLLECT_LOGS", Value: logsEnabled},
		{Name: "LOG_SINCE_HOURS", Value: fmt.Sprintf("%d", sinceHours)},
		{Name: "LOG_CONCURRENCY", Value: fmt.Sprintf("%d", concurrency)},
		{Name: "MAX_BYTES_PER_POD", Value: fmt.Sprintf("%d", agent.Spec.Collection.Logs.MaxBytesPerPod)},
		{Name: "LOG_MAX_TAIL_LINES", Value: fmt.Sprintf("%d", maxTailLines)},
		{Name: "VICTORIA_LOGS_URL", Value: agent.Spec.Collection.Logs.VictoriaLogsURL},
		{Name: "VL_HISTORY_SECONDS", Value: fmt.Sprintf("%d", vlHistoryDays*86400)},
		{Name: "COLLECT_METRICS", Value: metricsEnabled},
		{Name: "VICTORIA_METRICS_URL", Value: agent.Spec.Collection.Metrics.VictoriaMetricsURL},
		{Name: "METRICS_RANGE_SECONDS", Value: fmt.Sprintf("%d", rangeSeconds)},
		{Name: "METRICS_STEP", Value: step},
	}

	if len(agent.Spec.Collection.Logs.ExcludeNamespaces) > 0 {
		env = append(env, corev1.EnvVar{
			Name:  "EXCLUDE_NAMESPACES",
			Value: strings.Join(agent.Spec.Collection.Logs.ExcludeNamespaces, ","),
		})
	}

	if agent.Spec.APIKey != nil {
		env = append(env, corev1.EnvVar{
			Name: "API_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: agent.Spec.APIKey,
			},
		})
	}

	// User-defined extra env vars (can override above).
	if agent.Spec.AgentOverride != nil {
		env = append(env, agent.Spec.AgentOverride.Env...)
	}

	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	if agent.Spec.AgentOverride != nil && agent.Spec.AgentOverride.Resources != nil {
		resources = *agent.Spec.AgentOverride.Resources
	}

	return corev1.Container{
		Name:            "agent",
		Image:           resolveImage(agent),
		ImagePullPolicy: resolveImagePullPolicy(agent),
		Env:             env,
		Resources:       resources,
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   boolPtr(false), // agent writes temp files
			AllowPrivilegeEscalation: boolPtr(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		},
	}
}

// ─── Pointer helpers ─────────────────────────────────────────────────────────

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }
