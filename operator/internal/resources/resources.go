// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package resources

import (
	"context"
	"fmt"

	resourcesv1 "dragonflydb.io/dragonfly/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GetDatabaseResources returns the resources required for a DragonflyDb
// Instance
func GetDatabaseResources(ctx context.Context, db *resourcesv1.DragonflyDb) ([]client.Object, error) {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Creating resources for %s", db.Name))

	var resources []client.Object

	image := db.Spec.Image
	if image == "" {
		image = fmt.Sprintf("%s:%s", DragonflyDbImage, Version)
	}

	// Master + Replicas
	replicas := db.Spec.Replicas + 1

	// Create a StatefulSet, Headless Service
	statefulset := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			// Useful for automatically deleting the resources when the Database object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: db.APIVersion,
					Kind:       db.Kind,
					Name:       db.Name,
					UID:        db.UID,
				},
			},
			Labels: map[string]string{
				KubernetesAppComponentLabelKey: "database",
				KubernetesAppInstanceNameLabel: db.Name,
				KubernetesAppNameLabelKey:      "dragonflydb",
				KubernetesAppVersionLabelKey:   Version,
				KubernetesPartOfLabelKey:       "dragonflydb",
				KubernetesManagedByLabelKey:    DragonflyDbOperatorName,
				"app":                          db.Name,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: db.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                     db.Name,
					KubernetesPartOfLabelKey:  "dragonflydb",
					KubernetesAppNameLabelKey: "dragonflydb",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                     db.Name,
						KubernetesPartOfLabelKey:  "dragonflydb",
						KubernetesAppNameLabelKey: "dragonflydb",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "dragonflydb",
							Image: image,
							Ports: []corev1.ContainerPort{
								{
									Name:          DragonflyDbPortName,
									ContainerPort: DragonflyDbPort,
								},
							},
							Args: []string{
								"--alsologtostderr",
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"/usr/local/bin/healthcheck.sh",
										},
									},
								},
								FailureThreshold:    3,
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"/usr/local/bin/healthcheck.sh",
										},
									},
								},
								FailureThreshold:    3,
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							ImagePullPolicy: corev1.PullAlways,
						},
					},
				},
			},
		},
	}

	resources = append(resources, &statefulset)

	service := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			// Useful for automatically deleting the resources when the Database object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: db.APIVersion,
					Kind:       db.Kind,
					Name:       db.Name,
					UID:        db.UID,
				},
			},
			Labels: map[string]string{
				KubernetesAppComponentLabelKey: "database",
				KubernetesAppInstanceNameLabel: db.Name,
				KubernetesAppNameLabelKey:      "dragonflydb",
				KubernetesAppVersionLabelKey:   Version,
				KubernetesPartOfLabelKey:       "dragonflydb",
				KubernetesManagedByLabelKey:    DragonflyDbOperatorName,
				"app":                          db.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"app":                     db.Name,
				KubernetesAppNameLabelKey: "dragonflydb",
				"role":                    "master",
			},
			Ports: []corev1.ServicePort{
				{
					Name: DragonflyDbPortName,
					Port: DragonflyDbPort,
				},
			},
		},
	}

	resources = append(resources, &service)

	return resources, nil
}
