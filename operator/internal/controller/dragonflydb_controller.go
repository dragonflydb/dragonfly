// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package controller

import (
	"context"
	"fmt"
	"time"

	dfdb "dragonflydb.io/dragonfly/api/v1alpha1"
	"dragonflydb.io/dragonfly/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DragonflyDbReconciler reconciles a DragonflyDb object
type DragonflyDbReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflydbs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflydbs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflydbs/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DragonflyDb object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DragonflyDbReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var db dfdb.DragonflyDb
	if err := r.Get(ctx, req.NamespacedName, &db); err != nil {
		log.Info(fmt.Sprintf("could not get the Database object: %s", req.NamespacedName))
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling Database object")
	// Ignore if resource is already created
	// TODO: Handle updates to the Database object
	if !db.Status.Created {
		log.Info("Creating resources")
		resources, err := resources.GetDatabaseResources(ctx, &db)
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
		if err := waitForStatefulSetReady(ctx, r.Client, db.Name, db.Namespace, 2*time.Minute); err != nil {
			log.Error(err, "could not wait for statefulset to be ready")
			return ctrl.Result{}, err
		}

		if err := findHealthyAndMarkActive(ctx, r.Client, &db); err != nil {
			log.Error(err, "could not find healthy and mark active")
			return ctrl.Result{}, err
		}

		// Update Status
		db.Status.Created = true
		log.Info("Created resources for object")
		if err := r.Status().Update(ctx, &db); err != nil {
			log.Error(err, "could not update the Database object")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DragonflyDbReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfdb.DragonflyDb{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
