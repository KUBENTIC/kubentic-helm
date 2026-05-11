package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubenticv1alpha1 "github.com/kubentic/operator/api/v1alpha1"
)

const (
	finalizerName = "kubentic.io/finalizer"

	defaultAgentImage = "ghcr.io/kubentic/agent:latest"
	defaultSchedule   = "0 * * * *"
	defaultBackendURL = "https://pa.kubentic.ai:8443"
)

// KubenticAgentReconciler reconciles KubenticAgent objects.
type KubenticAgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=kubentic.io,resources=kubenticagents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubentic.io,resources=kubenticagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubentic.io,resources=kubenticagents/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *KubenticAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("kubenticagent", req.NamespacedName)

	agent := &kubenticv1alpha1.KubenticAgent{}
	if err := r.Get(ctx, req.NamespacedName, agent); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: run cleanup then remove finalizer.
	if !agent.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, agent)
	}

	// Ensure the finalizer is present so we can clean up on deletion.
	if !controllerutil.ContainsFinalizer(agent, finalizerName) {
		controllerutil.AddFinalizer(agent, finalizerName)
		if err := r.Update(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile each owned resource in order.
	if agent.Spec.CreateRbac {
		if err := r.reconcileServiceAccount(ctx, agent); err != nil {
			return r.setDegraded(ctx, agent, "ServiceAccountFailed", err)
		}
		if err := r.reconcileClusterRole(ctx, agent); err != nil {
			return r.setDegraded(ctx, agent, "ClusterRoleFailed", err)
		}
		if err := r.reconcileClusterRoleBinding(ctx, agent); err != nil {
			return r.setDegraded(ctx, agent, "ClusterRoleBindingFailed", err)
		}
	}

	if err := r.reconcileCronJob(ctx, agent); err != nil {
		return r.setDegraded(ctx, agent, "CronJobFailed", err)
	}

	// Sync lastScanTime / nextScanTime from the CronJob's own status.
	if err := r.syncScanTimes(ctx, agent); err != nil {
		logger.Error(err, "failed to sync scan times (non-fatal)")
	}

	// Mark as ready.
	if err := r.setReady(ctx, agent); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Reconciled successfully")
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// handleDeletion removes all resources owned by this agent, then strips the finalizer.
func (r *KubenticAgentReconciler) handleDeletion(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Running finalizer cleanup")

	if err := r.deleteCronJob(ctx, agent); err != nil {
		return ctrl.Result{}, err
	}
	if agent.Spec.CreateRbac {
		if err := r.deleteClusterRoleBinding(ctx, agent); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.deleteClusterRole(ctx, agent); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.deleteServiceAccount(ctx, agent); err != nil {
			return ctrl.Result{}, err
		}
	}

	controllerutil.RemoveFinalizer(agent, finalizerName)
	return ctrl.Result{}, r.Update(ctx, agent)
}

// ─── ServiceAccount ──────────────────────────────────────────────────────────

func (r *KubenticAgentReconciler) reconcileServiceAccount(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	sa := buildServiceAccount(agent)
	if err := controllerutil.SetControllerReference(agent, sa, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on ServiceAccount: %w", err)
	}
	existing := &corev1.ServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{Name: sa.Name, Namespace: sa.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, sa)
	}
	if err != nil {
		return err
	}
	existing.Labels = sa.Labels
	existing.Annotations = sa.Annotations
	existing.OwnerReferences = sa.OwnerReferences
	return r.Update(ctx, existing)
}

func (r *KubenticAgentReconciler) deleteServiceAccount(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	sa := &corev1.ServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{Name: agentSAName(agent), Namespace: agent.Namespace}, sa)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Delete(ctx, sa)
}

// ─── ClusterRole ─────────────────────────────────────────────────────────────

func (r *KubenticAgentReconciler) reconcileClusterRole(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	cr := buildClusterRole(agent)
	existing := &rbacv1.ClusterRole{}
	err := r.Get(ctx, types.NamespacedName{Name: cr.Name}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, cr)
	}
	if err != nil {
		return err
	}
	existing.Rules = cr.Rules
	existing.Labels = cr.Labels
	return r.Update(ctx, existing)
}

func (r *KubenticAgentReconciler) deleteClusterRole(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	cr := &rbacv1.ClusterRole{}
	err := r.Get(ctx, types.NamespacedName{Name: agentClusterRoleName(agent)}, cr)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Delete(ctx, cr)
}

// ─── ClusterRoleBinding ──────────────────────────────────────────────────────

func (r *KubenticAgentReconciler) reconcileClusterRoleBinding(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	crb := buildClusterRoleBinding(agent)
	existing := &rbacv1.ClusterRoleBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: crb.Name}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, crb)
	}
	if err != nil {
		return err
	}
	existing.Subjects = crb.Subjects
	existing.RoleRef = crb.RoleRef
	existing.Labels = crb.Labels
	return r.Update(ctx, existing)
}

func (r *KubenticAgentReconciler) deleteClusterRoleBinding(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	crb := &rbacv1.ClusterRoleBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: agentCRBName(agent)}, crb)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Delete(ctx, crb)
}

// ─── CronJob ─────────────────────────────────────────────────────────────────

func (r *KubenticAgentReconciler) reconcileCronJob(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	desired := buildCronJob(agent)
	if err := controllerutil.SetControllerReference(agent, desired, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on CronJob: %w", err)
	}
	existing := &batchv1.CronJob{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Patch mutable fields only (schedule, suspend, job template).
	existing.Spec.Schedule = desired.Spec.Schedule
	existing.Spec.Suspend = desired.Spec.Suspend
	existing.Spec.JobTemplate = desired.Spec.JobTemplate
	existing.Spec.ConcurrencyPolicy = desired.Spec.ConcurrencyPolicy
	existing.Spec.SuccessfulJobsHistoryLimit = desired.Spec.SuccessfulJobsHistoryLimit
	existing.Spec.FailedJobsHistoryLimit = desired.Spec.FailedJobsHistoryLimit
	return r.Update(ctx, existing)
}

func (r *KubenticAgentReconciler) deleteCronJob(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	cj := &batchv1.CronJob{}
	err := r.Get(ctx, types.NamespacedName{Name: agentCronJobName(agent), Namespace: agent.Namespace}, cj)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Delete(ctx, cj)
}

// ─── Status helpers ──────────────────────────────────────────────────────────

// syncScanTimes reads the managed CronJob's status to stamp lastScanTime and
// computes the next fire time from the cron schedule for nextScanTime.
func (r *KubenticAgentReconciler) syncScanTimes(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	cj := &batchv1.CronJob{}
	if err := r.Get(ctx, types.NamespacedName{Name: agentCronJobName(agent), Namespace: agent.Namespace}, cj); err != nil {
		return err
	}

	// lastScanTime — from the CronJob's own LastSuccessfulTime field.
	if cj.Status.LastSuccessfulTime != nil {
		agent.Status.LastScanTime = cj.Status.LastSuccessfulTime.DeepCopy()
	}

	// nextScanTime — parse the cron schedule and compute the next fire after now.
	schedule := resolveSchedule(agent)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parsed, err := parser.Parse(schedule)
	if err != nil {
		return fmt.Errorf("parsing cron schedule %q: %w", schedule, err)
	}
	next := parsed.Next(time.Now().UTC())
	if !next.IsZero() {
		t := metav1.NewTime(next)
		agent.Status.NextScanTime = &t
	}

	return nil
}

func (r *KubenticAgentReconciler) setReady(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent) error {
	phase := kubenticv1alpha1.PhaseRunning
	if agent.Spec.Suspend {
		phase = kubenticv1alpha1.PhaseSuspended
	}

	agent.Status.Phase = phase
	agent.Status.ObservedGeneration = agent.Generation
	agent.Status.CronJobName = agentCronJobName(agent)
	agent.Status.AgentVersion = agentImageTag(agent)

	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               kubenticv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "All resources reconciled successfully",
		ObservedGeneration: agent.Generation,
	})
	meta.RemoveStatusCondition(&agent.Status.Conditions, kubenticv1alpha1.ConditionTypeDegraded)

	if agent.Spec.Suspend {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               kubenticv1alpha1.ConditionTypeSuspended,
			Status:             metav1.ConditionTrue,
			Reason:             "Suspended",
			Message:            "Collection is suspended via spec.suspend",
			ObservedGeneration: agent.Generation,
		})
	} else {
		meta.RemoveStatusCondition(&agent.Status.Conditions, kubenticv1alpha1.ConditionTypeSuspended)
	}

	return r.Status().Update(ctx, agent)
}

func (r *KubenticAgentReconciler) setDegraded(ctx context.Context, agent *kubenticv1alpha1.KubenticAgent, reason string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Error(cause, "Setting degraded status", "reason", reason)
	agent.Status.Phase = kubenticv1alpha1.PhaseFailed
	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               kubenticv1alpha1.ConditionTypeDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: agent.Generation,
	})
	if err := r.Status().Update(ctx, agent); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, cause
}

// ─── SetupWithManager ────────────────────────────────────────────────────────

func (r *KubenticAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubenticv1alpha1.KubenticAgent{}).
		Owns(&batchv1.CronJob{}).
		Owns(&corev1.ServiceAccount{}).
		Complete(r)
}
