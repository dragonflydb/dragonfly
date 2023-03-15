// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package controller

import (
	"context"
	"fmt"
	"time"

	dfv1alpha1 "dragonflydb.io/dragonfly/api/v1alpha1"
	"dragonflydb.io/dragonfly/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DragonflyReconciler reconciles a Dragonfly object
type DragonflyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Dragonfly object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DragonflyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var df dfv1alpha1.Dragonfly
	if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
		log.Info(fmt.Sprintf("could not get the Dragonfly object: %s", req.NamespacedName))
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling Dragonfly object")
	// Ignore if resource is already created
	// TODO: Handle updates to the Dragonfly object
	if !df.Status.Created {
		log.Info("Creating resources")
		resources, err := resources.GetDragonflyResources(ctx, &df)
		if err != nil {
			log.Error(err, "could not get resources")
			return ctrl.Result{}, err
		}

		// create all resources
		for _, resource := range resources {
			if err := r.Create(ctx, resource); err != nil {
				log.Error(err, "could not create resource")
				return ctrl.Result{}, err
			}
		}

		log.Info("Waiting for the statefulset to be ready")
		if err := waitForStatefulSetReady(ctx, r.Client, df.Name, df.Namespace, 2*time.Minute); err != nil {
			log.Error(err, "could not wait for statefulset to be ready")
			return ctrl.Result{}, err
		}

		if err := findHealthyAndMarkActive(ctx, r.Client, &df); err != nil {
			log.Error(err, "could not find healthy and mark active")
			return ctrl.Result{}, err
		}

		// Update Status
		df.Status.Created = true
		log.Info("Created resources for object")
		if err := r.Status().Update(ctx, &df); err != nil {
			log.Error(err, "could not update the Dragonfly object")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DragonflyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfv1alpha1.Dragonfly{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
