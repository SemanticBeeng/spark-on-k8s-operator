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

package main

import (
	"flag"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/runtime/signals"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd"
	ssacrd "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd/scheduledsparkapplication"
	sacrd "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd/sparkapplication"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	wb "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/webhook"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	// +kubebuilder:scaffold:imports
)

var (
	installCRDs      = flag.Bool("install-crds", true, "Whether to install CRDs")
	resyncInterval   = flag.Int("resync-interval", 30, "Informer resync interval in seconds.")
	namespace        = flag.String("namespace", apiv1.NamespaceAll, "The Kubernetes namespace(s) to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset. Multiple namespace can be seperated with comma.")
	enableWebhook    = flag.Bool("enable-webhook", false, "Whether to enable the mutating admission webhook for admitting and patching Spark pods.")
	enableMetrics    = flag.Bool("enable-metrics", false, "Whether to enable the metrics endpoint.")
	metricsPort      = flag.String("metrics-port", "10254", "Port for the metrics endpoint.")
	metricsEndpoint  = flag.String("metrics-endpoint", "/metrics", "Metrics endpoint.")
	metricsPrefix    = flag.String("metrics-prefix", "", "Prefix for the metrics.")
	ingressUrlFormat = flag.String("ingress-url-format", "", "Ingress URL format.")
	setupLog         = ctrl.Log.WithName("setup")
)

func main() {
	var metricsLabels util.ArrayFlags
	var metricsAddr string
	flag.Var(&metricsLabels, "metrics-labels", "Labels for the metrics")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.Parse()

	// Create the client config. Use kubeConfig if given, otherwise assume in-cluster.
	config, err := ctrl.GetConfig()
	if err != nil {
		glog.Fatal(err)
	}

	var metricConfig *util.MetricConfig
	if *enableMetrics {
		metricConfig = &util.MetricConfig{
			MetricsEndpoint: *metricsEndpoint,
			MetricsPort:     *metricsPort,
			MetricsPrefix:   *metricsPrefix,
			MetricsLabels:   metricsLabels,
		}

		glog.Info("Enabling metrics collecting and exporting to Prometheus")
		util.InitializeMetrics(metricConfig)
	}

	ctrl.SetLogger(zap.Logger(true))
	setupLog.Info("Starting the Spark Operator")

	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		setupLog.Error(err, "Unable to get a client")
	}

	if *installCRDs {
		err = crd.CreateOrUpdateCRD(apiExtensionsClient, sacrd.GetCRD())
		if err != nil {
			glog.Fatalf("failed to create or update CustomResourceDefinition %s: %v", sacrd.FullName, err)
		}

		err = crd.CreateOrUpdateCRD(apiExtensionsClient, ssacrd.GetCRD())
		if err != nil {
			glog.Fatalf("failed to create or update CustomResourceDefinition %s: %v", ssacrd.FullName, err)
		}
	}

	// Create a new Cmd to provide shared dependencies and start components
	glog.Info("Setting up the controller runtime manager")
	syncPeriodDuration := time.Duration(*resyncInterval) * time.Second

	var mgr manager.Manager

	if *namespace == "" {
		mgr, err = manager.New(config, manager.Options{
			SyncPeriod: &syncPeriodDuration,
		})
	} else {
		namespaceList := strings.Split(*namespace, ",")
		mgr, err = manager.New(config, manager.Options{
			NewCache:   cache.MultiNamespacedCacheBuilder(namespaceList),
			SyncPeriod: &syncPeriodDuration,
		})
	}

	if err != nil {
		glog.Error(err, "unable to set up overall controller manager")
		os.Exit(1)
	}

	setupLog.Info("Registering Components.")

	// Setup Scheme for all resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		glog.Error(err, "Unable to register the reuqired schemes")
		os.Exit(1)
	}

	// Setup all Controllers
	setupLog.Info("Adding Controllers.")
	if err := controller.AddToManager(mgr, metricConfig); err != nil {
		glog.Error(err, "Unable to add the controllers")
		os.Exit(1)
	}

	if *enableWebhook {
		setupLog.Info("Setting up webhooks")
		if err := wb.AddToManager(mgr); err != nil {
			glog.Error(err, "unable to register webhooks to the manager")
			os.Exit(1)
		}

		// +kubebuilder:scaffold:builder

		setupLog.Info("Getting the webhook server")
		hookServer := mgr.GetWebhookServer()
		hookServer.Register("/mutate-v1-pod", &webhook.Admission{Handler: &wb.SparkPodMutator{JobNameSpace: *namespace}})
	}

	//Start the Cmd
	setupLog.Info("Starting the Cmd.")
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		glog.Error(err, "unable to run the manager")
		os.Exit(1)
	}
}
