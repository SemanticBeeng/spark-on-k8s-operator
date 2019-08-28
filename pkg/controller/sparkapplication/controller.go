/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"context"
	"fmt"
	v1 "k8s.io/api/apps/v1"
	v1beta12 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/lyft/flytestdlib/contextutils"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	sparkExecutorIDLabel      = "spark-exec-id"
	podAlreadyExistsErrorCode = "code=409"
	maximumUpdateRetries      = 3

	Deployment = "Deployment"
	Pod        = "Pod"
	Service    = "Service"
	Endpoints  = "Endpoints"
	Ingress    = "Ingress"
)

var (
	execCommand = exec.Command
)

// Add creates a new SparkApplication Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, metricsConfig *util.MetricConfig) error {
	return add(mgr, newReconciler(mgr, metricsConfig))
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("sparkapplication-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to SparkApplication
	err = c.Watch(&source.Kind{Type: &v1beta1.SparkApplication{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for Pod created by SparkApplication
	err = c.Watch(&source.Kind{Type: &apiv1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &v1beta1.SparkApplication{},
	})
	if err != nil {
		return err
	}

	return nil
}

func newReconciler(mgr manager.Manager, metricsConfig *util.MetricConfig) reconcile.Reconciler {
	reconciler := &ReconcileSparkApplication{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
	}

	if metricsConfig != nil {
		reconciler.metrics = newSparkAppMetrics(metricsConfig.MetricsPrefix, metricsConfig.MetricsLabels)
		reconciler.metrics.registerMetrics()
	}
	return reconciler
}

var _ reconcile.Reconciler = &ReconcileSparkApplication{}

// ReconcileSparkApplication reconciles a SparkApplication object
type ReconcileSparkApplication struct {
	client           client.Client
	scheme           *runtime.Scheme
	metrics          *sparkAppMetrics
	recorder         record.EventRecorder
	crdClient        crdclientset.Interface
	ingressURLFormat string
}

func (r *ReconcileSparkApplication) recordSparkApplicationEvent(app *v1beta1.SparkApplication) {
	switch app.Status.AppState.State {
	case v1beta1.NewState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationAdded",
			"SparkApplication %s was added, enqueuing it for submission",
			app.Name)
	case v1beta1.SubmittedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationSubmitted",
			"SparkApplication %s was submitted successfully",
			app.Name)
	case v1beta1.FailedSubmissionState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"failed to submit SparkApplication %s: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.CompletedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationCompleted",
			"SparkApplication %s completed",
			app.Name)
	case v1beta1.FailedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationFailed",
			"SparkApplication %s failed: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.PendingRerunState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationPendingRerun",
			"SparkApplication %s is pending rerun",
			app.Name)
	}
}

func (r *ReconcileSparkApplication) recordDriverEvent(app *v1beta1.SparkApplication, phase apiv1.PodPhase, name string) {
	switch phase {
	case apiv1.PodSucceeded:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	case apiv1.PodPending:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverPending", "Driver %s is pending", name)
	case apiv1.PodRunning:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverRunning", "Driver %s is running", name)
	case apiv1.PodFailed:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	case apiv1.PodUnknown:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverUnknownState", "Driver %s in unknown state", name)
	}
}

func (r *ReconcileSparkApplication) recordExecutorEvent(app *v1beta1.SparkApplication, state v1beta1.ExecutorState, name string) {
	switch state {
	case v1beta1.ExecutorCompletedState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", name)
	case v1beta1.ExecutorPendingState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorPending", "Executor %s is pending", name)
	case v1beta1.ExecutorRunningState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorRunning", "Executor %s is running", name)
	case v1beta1.ExecutorFailedState:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed", name)
	case v1beta1.ExecutorUnknownState:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorUnknownState", "Executor %s in unknown state", name)
	}
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (r *ReconcileSparkApplication) submitSparkApplication(ctx context.Context, app *v1beta1.SparkApplication) *v1beta1.SparkApplication {
	if app.PrometheusMonitoringEnabled() {
		if err := configPrometheusMonitoring(ctx, app, r.client); err != nil {
			glog.Error(err)
		}
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		app.Status = v1beta1.SparkApplicationStatus{
			AppState: v1beta1.ApplicationState{
				State:        v1beta1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
			LastSubmissionAttemptTime: metav1.Now(),
		}
		return app
	}

	// Try submitting the application by running spark-submit.
	submitted, err := runSparkSubmit(newSubmission(submissionCmdArgs, app))
	if err != nil {
		app.Status = v1beta1.SparkApplicationStatus{
			AppState: v1beta1.ApplicationState{
				State:        v1beta1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
			LastSubmissionAttemptTime: metav1.Now(),
		}
		r.recordSparkApplicationEvent(app)
		glog.Errorf("failed to run spark-submit for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		return app
	}
	if !submitted {
		// The application may not have been submitted even if err == nil, e.g., when some
		// state update caused an attempt to re-submit the application, in which case no
		// error gets returned from runSparkSubmit. If this is the case, we simply return.
		return app
	}

	glog.Infof("SparkApplication %s/%s has been submitted", app.Namespace, app.Name)
	app.Status = v1beta1.SparkApplicationStatus{
		SubmissionID: submissionID,
		AppState: v1beta1.ApplicationState{
			State: v1beta1.SubmittedState,
		},
		DriverInfo: v1beta1.DriverInfo{
			PodName: driverPodName,
		},
		SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
		ExecutionAttempts:         app.Status.ExecutionAttempts + 1,
		LastSubmissionAttemptTime: metav1.Now(),
	}
	r.recordSparkApplicationEvent(app)

	service, err := createSparkUIService(ctx, r.client, app)
	if err != nil {
		glog.Errorf("failed to create UI service for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	} else {
		app.Status.DriverInfo.WebUIServiceName = service.serviceName
		app.Status.DriverInfo.WebUIPort = service.servicePort
		app.Status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", service.serviceIP, app.Status.DriverInfo.WebUIPort)
		// Create UI Ingress if ingress-format is set.
		if r.ingressURLFormat != "" {
			ingress, err := createSparkUIIngress(ctx, app, *service, r.ingressURLFormat, r.client)
			if err != nil {
				glog.Errorf("failed to create UI Ingress for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
			} else {
				app.Status.DriverInfo.WebUIIngressAddress = ingress.ingressURL
				app.Status.DriverInfo.WebUIIngressName = ingress.ingressName
			}
		}
	}
	return app
}

func (r *ReconcileSparkApplication) updateApplicationStatusWithRetries(
	original *v1beta1.SparkApplication,
	updateFunc func(status *v1beta1.SparkApplicationStatus)) (*v1beta1.SparkApplication, error) {
	toUpdate := original.DeepCopy()

	var lastUpdateErr error
	for i := 0; i < maximumUpdateRetries; i++ {
		updateFunc(&toUpdate.Status)
		if equality.Semantic.DeepEqual(original.Status, toUpdate.Status) {
			return toUpdate, nil
		}
		_, err := r.crdClient.SparkoperatorV1beta1().SparkApplications(toUpdate.Namespace).Update(toUpdate)
		if err == nil {
			return toUpdate, nil
		}

		lastUpdateErr = err

		// Failed to update to the API server.
		// Get the latest version from the API server first and re-apply the update.
		name := toUpdate.Name
		toUpdate, err = r.crdClient.SparkoperatorV1beta1().SparkApplications(toUpdate.Namespace).Get(name,
			metav1.GetOptions{})
		if err != nil {
			glog.Errorf("failed to get SparkApplication %s/%s: %v", original.Namespace, name, err)
			return nil, err
		}
	}

	if lastUpdateErr != nil {
		glog.Errorf("failed to update SparkApplication %s/%s: %v", original.Namespace, original.Name, lastUpdateErr)
		return nil, lastUpdateErr
	}

	return toUpdate, nil
}

// updateStatusAndExportMetrics updates the status of the SparkApplication and export the metrics.
func (r *ReconcileSparkApplication) updateStatusAndExportMetrics(oldApp, newApp *v1beta1.SparkApplication) error {
	// Skip update if nothing changed.
	if equality.Semantic.DeepEqual(oldApp, newApp) {
		return nil
	}

	updatedApp, err := r.updateApplicationStatusWithRetries(oldApp, func(status *v1beta1.SparkApplicationStatus) {
		*status = newApp.Status
	})

	// Export metrics if the update was successful.
	if err == nil && r.metrics != nil {
		r.metrics.exportMetrics(oldApp, updatedApp)
	}

	return err
}

func (r *ReconcileSparkApplication) getSparkApplication(ctx context.Context, namespace string, name string) (*v1beta1.SparkApplication, error) {
	namespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	sparkapp := &v1beta1.SparkApplication{}
	err := r.client.Get(ctx, namespacedName, sparkapp)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return sparkapp, nil
}

// Delete the driver pod and optional UI resources (Service/Ingress) created for the application.
func (r *ReconcileSparkApplication) deleteSparkResources(ctx context.Context, app *v1beta1.SparkApplication) error {
	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName != "" {
		glog.V(2).Infof("Deleting pod %s in namespace %s", driverPodName, app.Namespace)
		pod, err := r.getDriverPod(ctx, app)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		err = r.client.Delete(ctx, pod, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		glog.V(2).Infof("Deleting Spark UI Service %s in namespace %s", sparkUIServiceName, app.Namespace)
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIServiceName,
		}
		svc := &apiv1.Service{}
		err := r.client.Get(ctx, namespacedName, svc)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		err = r.client.Delete(ctx, svc, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		glog.V(2).Infof("Deleting Spark UI Ingress %s in namespace %s", sparkUIIngressName, app.Namespace)
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIIngressName,
		}
		ingress := &v1beta12.Ingress{}
		err := r.client.Get(ctx, namespacedName, ingress)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		err = r.client.Delete(ctx, ingress, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (r *ReconcileSparkApplication) validateSparkApplication(app *v1beta1.SparkApplication) error {
	appSpec := app.Spec
	driverSpec := appSpec.Driver
	executorSpec := appSpec.Executor
	if appSpec.NodeSelector != nil && (driverSpec.NodeSelector != nil || executorSpec.NodeSelector != nil) {
		return fmt.Errorf("NodeSelector property can be defined at SparkApplication or at any of Driver,Executor")
	}

	return nil
}

// Validate that any Spark resources (driver/Service/Ingress) created for the application have been deleted.
func (r *ReconcileSparkApplication) validateSparkResourceDeletion(ctx context.Context, app *v1beta1.SparkApplication) bool {
	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      driverPodName,
		}
		pod := &apiv1.Pod{}
		err := r.client.Get(ctx, namespacedName, pod)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIServiceName,
		}
		svc := &apiv1.Service{}
		err := r.client.Get(ctx, namespacedName, svc)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIIngressName,
		}
		ingress := &v1beta12.Ingress{}
		err := r.client.Get(ctx, namespacedName, ingress)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	return true
}

func (r *ReconcileSparkApplication) clearStatus(status *v1beta1.SparkApplicationStatus) {
	if status.AppState.State == v1beta1.InvalidatingState {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.ExecutionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.TerminationTime = metav1.Time{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	} else if status.AppState.State == v1beta1.PendingRerunState {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.DriverInfo = v1beta1.DriverInfo{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	}
}

func (r *ReconcileSparkApplication) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx := context.Background()
	ctx = contextutils.WithNamespace(ctx, request.Namespace)
	ctx = contextutils.WithAppName(ctx, request.Name)

	// Fetch the SparkApplication
	sparkapp := &v1beta1.SparkApplication{}
	err := r.client.Get(ctx, request.NamespacedName, sparkapp)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return. Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	appToUpdate := sparkapp.DeepCopy()
	glog.Infof("Current Spark app state: %s", appToUpdate.Status.AppState.State)

	switch appToUpdate.Status.AppState.State {
	case v1beta1.NewState:
		v1beta1.SetSparkApplicationDefaults(appToUpdate)
		glog.Infof("SparkApplication %s/%s was added", appToUpdate.Namespace, appToUpdate.Name)
		r.recordSparkApplicationEvent(appToUpdate)
		if err := r.validateSparkApplication(appToUpdate); err != nil {
			appToUpdate.Status.AppState.State = v1beta1.FailedState
			appToUpdate.Status.AppState.ErrorMessage = err.Error()
		} else {
			appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
		}
	case v1beta1.SucceedingState:
		if !shouldRetry(appToUpdate) {
			// Application is not subject to retry. Move to terminal CompletedState.
			appToUpdate.Status.AppState.State = v1beta1.CompletedState
			r.recordSparkApplicationEvent(appToUpdate)
		} else {
			if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
				glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
					appToUpdate.Namespace, appToUpdate.Name, err)
				return reconcile.Result{}, err
			}
			appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
		}
	case v1beta1.FailingState:
		if !shouldRetry(appToUpdate) {
			// Application is not subject to retry. Move to terminal FailedState.
			appToUpdate.Status.AppState.State = v1beta1.FailedState
			r.recordSparkApplicationEvent(appToUpdate)
		} else if hasRetryIntervalPassed(appToUpdate.Spec.RestartPolicy.OnFailureRetryInterval, appToUpdate.Status.ExecutionAttempts, appToUpdate.Status.TerminationTime) {
			if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
				glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
					appToUpdate.Namespace, appToUpdate.Name, err)
				return reconcile.Result{}, err
			}
			appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
		}
	case v1beta1.FailedSubmissionState:
		if !shouldRetry(appToUpdate) {
			// App will never be retried. Move to terminal FailedState.
			appToUpdate.Status.AppState.State = v1beta1.FailedState
			r.recordSparkApplicationEvent(appToUpdate)
		} else if hasRetryIntervalPassed(appToUpdate.Spec.RestartPolicy.OnSubmissionFailureRetryInterval, appToUpdate.Status.SubmissionAttempts, appToUpdate.Status.LastSubmissionAttemptTime) {
			appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
		}
	case v1beta1.InvalidatingState:
		// Invalidate the current run and enqueue the SparkApplication for re-execution.
		if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
			glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
				appToUpdate.Namespace, appToUpdate.Name, err)
			return reconcile.Result{}, err
		}
		r.clearStatus(&appToUpdate.Status)
		appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
	case v1beta1.PendingRerunState:
		glog.V(2).Infof("SparkApplication %s/%s pending rerun", appToUpdate.Namespace, appToUpdate.Name)
		if r.validateSparkResourceDeletion(ctx, appToUpdate) {
			glog.V(2).Infof("Resources for SparkApplication %s/%s successfully deleted", appToUpdate.Namespace, appToUpdate.Name)
			r.recordSparkApplicationEvent(appToUpdate)
			r.clearStatus(&appToUpdate.Status)
			appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
		}
	case v1beta1.SubmittedState, v1beta1.RunningState, v1beta1.UnknownState:
		if err := r.getAndUpdateAppState(ctx, appToUpdate); err != nil {
			return reconcile.Result{}, err
		}
	}

	if appToUpdate != nil {
		glog.V(2).Infof("Trying to update SparkApplication %s/%s, from: [%v] to [%v]", sparkapp.Namespace, sparkapp.Name, sparkapp.Status, appToUpdate.Status)
		err = r.updateStatusAndExportMetrics(sparkapp, appToUpdate)
		if err != nil {
			glog.Errorf("failed to update SparkApplication %s/%s: %v", appToUpdate.Namespace, appToUpdate.Name, err)
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileSparkApplication) getExecutorPods(ctx context.Context, app *v1beta1.SparkApplication) (*apiv1.PodList, error) {
	matchLabels := getResourceLabels(app)
	matchLabels[config.SparkRoleLabel] = config.SparkExecutorRole

	// Fetch all the executor pods for the current run of the application.
	podList := &apiv1.PodList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Pod,
		},
	}

	err := r.client.List(ctx, podList, client.MatchingLabels(matchLabels), client.InNamespace(app.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
	return podList, nil
}

func (r *ReconcileSparkApplication) getDriverPod(ctx context.Context, app *v1beta1.SparkApplication) (*apiv1.Pod, error) {
	namespacedName := types.NamespacedName{
		Namespace: app.Namespace,
		Name:      app.Status.DriverInfo.PodName,
	}
	pod := &apiv1.Pod{}
	err := r.client.Get(ctx, namespacedName, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("driver pod %s: %v not found", app.Status.DriverInfo.PodName, err)
		}
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}
	return pod, nil
}

// getAndUpdateDriverState finds the driver pod of the application
// and updates the driver state based on the current phase of the pod.
func (r *ReconcileSparkApplication) getAndUpdateDriverState(ctx context.Context, app *v1beta1.SparkApplication) error {
	// Either the driver pod doesn't exist yet or its name has not been updated.
	if app.Status.DriverInfo.PodName == "" {
		return fmt.Errorf("empty driver pod name with application state %s", app.Status.AppState.State)
	}

	driverPod, err := r.getDriverPod(ctx, app)
	if err != nil {
		return err
	}

	if driverPod == nil {
		app.Status.AppState.ErrorMessage = "Driver Pod not found"
		app.Status.AppState.State = v1beta1.FailingState
		app.Status.TerminationTime = metav1.Now()
		return nil
	}

	app.Status.SparkApplicationID = getSparkApplicationID(*driverPod)

	if driverPod.Status.Phase == apiv1.PodSucceeded || driverPod.Status.Phase == apiv1.PodFailed {
		if app.Status.TerminationTime.IsZero() {
			app.Status.TerminationTime = metav1.Now()
		}
		if driverPod.Status.Phase == apiv1.PodFailed {
			if len(driverPod.Status.ContainerStatuses) > 0 {
				terminatedState := driverPod.Status.ContainerStatuses[0].State.Terminated
				if terminatedState != nil {
					app.Status.AppState.ErrorMessage = fmt.Sprintf("driver pod failed with ExitCode: %d, Reason: %s", terminatedState.ExitCode, terminatedState.Reason)
				}
			} else {
				app.Status.AppState.ErrorMessage = "driver container status missing"
			}
		}
	}

	newState := driverPodPhaseToApplicationState(driverPod.Status.Phase)
	// Only record a driver event if the application state (derived from the driver pod phase) has changed.
	if newState != app.Status.AppState.State {
		r.recordDriverEvent(app, driverPod.Status.Phase, driverPod.Name)
	}
	app.Status.AppState.State = newState

	return nil
}

// getAndUpdateExecutorState lists the executor pods of the application
// and updates the executor state based on the current phase of the pods.
func (r *ReconcileSparkApplication) getAndUpdateExecutorState(ctx context.Context, app *v1beta1.SparkApplication) error {
	podList, err := r.getExecutorPods(ctx, app)
	if err != nil {
		return err
	}

	executorStateMap := make(map[string]v1beta1.ExecutorState)
	var executorApplicationID string
	for _, pod := range podList.Items {
		if util.IsExecutorPod(&pod) {
			newState := podPhaseToExecutorState(pod.Status.Phase)
			oldState, exists := app.Status.ExecutorState[pod.Name]
			// Only record an executor event if the executor state is new or it has changed.
			if !exists || newState != oldState {
				r.recordExecutorEvent(app, newState, pod.Name)
			}
			executorStateMap[pod.Name] = newState

			if executorApplicationID == "" {
				executorApplicationID = getSparkApplicationID(pod)
			}
		}
	}

	// ApplicationID label can be different on driver/executors. Prefer executor ApplicationID if set.
	// Refer https://issues.apache.org/jira/projects/SPARK/issues/SPARK-25922 for details.
	if executorApplicationID != "" {
		app.Status.SparkApplicationID = executorApplicationID
	}

	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1beta1.ExecutorState)
	}
	for name, execStatus := range executorStateMap {
		app.Status.ExecutorState[name] = execStatus
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !isExecutorTerminated(oldStatus) && !exists {
			glog.Infof("Executor pod %s not found, assuming it was deleted.", name)
			app.Status.ExecutorState[name] = v1beta1.ExecutorFailedState
		}
	}

	return nil
}

func (r *ReconcileSparkApplication) getAndUpdateAppState(ctx context.Context, app *v1beta1.SparkApplication) error {
	if err := r.getAndUpdateDriverState(ctx, app); err != nil {
		return err
	}
	if err := r.getAndUpdateExecutorState(ctx, app); err != nil {
		return err
	}
	return nil
}

func (r *ReconcileSparkApplication) handleSparkApplicationDeletion(ctx context.Context, app *v1beta1.SparkApplication) {
	// SparkApplication deletion requested, lets delete driver pod.
	if err := r.deleteSparkResources(ctx, app); err != nil {
		glog.Errorf("failed to delete resources associated with deleted SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
}

// ShouldRetry determines if SparkApplication in a given state should be retried.
func shouldRetry(app *v1beta1.SparkApplication) bool {
	switch app.Status.AppState.State {
	case v1beta1.SucceedingState:
		return app.Spec.RestartPolicy.Type == v1beta1.Always
	case v1beta1.FailingState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.ExecutionAttempts <= *app.Spec.RestartPolicy.OnFailureRetries {
				return true
			}
		}
	case v1beta1.FailedSubmissionState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts <= *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	}
	return false
}

// State Machine for SparkApplication:
//+--------------------------------------------------------------------------------------------------------------------+
//|                                                                                                                    |
//|                +---------+                                                                                         |
//|                |         |                                                                                         |
//|                |         +                                                                                         |
//|                |Submission                                                                                         |
//|           +----> Failed  +-----+------------------------------------------------------------------+                |
//|           |    |         |     |                                                                  |                |
//|           |    |         |     |                                                                  |                |
//|           |    +----^----+     |                                                                  |                |
//|           |         |          |                                                                  |                |
//|           |         |          |                                                                  |                |
//|      +----+----+    |    +-----v----+          +----------+           +-----------+          +----v-----+          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |		    |          |
//|      |   New   +---------> Submitted+----------> Running  +----------->  Failing  +---------->  Failed  |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      +---------+    |    +----^-----+          +-----+----+           +-----+-----+          +----------+          |
//|                     |         |                      |                      |                                      |
//|                     |         |                      |                      |                                      |
//|    +------------+   |         |             +-------------------------------+                                      |
//|    |            |   |   +-----+-----+       |        |                +-----------+          +----------+          |
//|    |            |   |   |  Pending  |       |        |                |           |          |          |          |
//|    |            |   +---+   Rerun   <-------+        +---------------->Succeeding +---------->Completed |          |
//|    |Invalidating|       |           <-------+                         |           |          |          |          |
//|    |            +------->           |       |                         |           |          |          |          |
//|    |            |       |           |       |                         |           |          |          |          |
//|    |            |       +-----------+       |                         +-----+-----+          +----------+          |
//|    +------------+                           |                               |                                      |
//|                                             |                               |                                      |
//|                                             +-------------------------------+                                      |
//|                                                                                                                    |
//+--------------------------------------------------------------------------------------------------------------------+

// Helper func to determine if we have waited enough to retry the SparkApplication.
func hasRetryIntervalPassed(retryInterval *int64, attemptsDone int32, lastEventTime metav1.Time) bool {
	glog.V(3).Infof("retryInterval: %d , lastEventTime: %v, attempsDone: %d", retryInterval, lastEventTime, attemptsDone)
	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	glog.V(3).Infof("currentTime is %v, interval is %v", currentTime, interval)
	if currentTime.After(lastEventTime.Add(interval)) {
		return true
	}
	return false
}
