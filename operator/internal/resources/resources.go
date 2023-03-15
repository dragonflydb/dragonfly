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

// GetDragonflyResources returns the resources required for a Dragonfly
// Instance
func GetDragonflyResources(ctx context.Context, db *resourcesv1.Dragonfly) ([]client.Object, error) {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Creating resources for %s", db.Name))

	var resources []client.Object

	image := db.Spec.Image
	if image == "" {
		image = fmt.Sprintf("%s:%s", DragonflyImage, Version)
	}

	// Master + Replicas
	replicas := db.Spec.Replicas + 1

	// Create a StatefulSet, Headless Service
	statefulset := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			// Useful for automatically deleting the resources when the Dragonfly object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: db.APIVersion,
					Kind:       db.Kind,
					Name:       db.Name,
					UID:        db.UID,
				},
			},
			Labels: map[string]string{
				KubernetesAppComponentLabelKey: "dragonfly",
				KubernetesAppInstanceNameLabel: db.Name,
				KubernetesAppNameLabelKey:      "dragonfly",
				KubernetesAppVersionLabelKey:   Version,
				KubernetesPartOfLabelKey:       "dragonfly",
				KubernetesManagedByLabelKey:    DragonflyOperatorName,
				"app":                          db.Name,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: db.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                     db.Name,
					KubernetesPartOfLabelKey:  "dragonfly",
					KubernetesAppNameLabelKey: "dragonfly",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                     db.Name,
						KubernetesPartOfLabelKey:  "dragonfly",
						KubernetesAppNameLabelKey: "dragonfly",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "dragonfly",
							Image: image,
							Ports: []corev1.ContainerPort{
								{
									Name:          DragonflyPortName,
									ContainerPort: DragonflyPort,
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
			// Useful for automatically deleting the resources when the Dragonfly object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: db.APIVersion,
					Kind:       db.Kind,
					Name:       db.Name,
					UID:        db.UID,
				},
			},
			Labels: map[string]string{
				KubernetesAppComponentLabelKey: "Dragonfly",
				KubernetesAppInstanceNameLabel: db.Name,
				KubernetesAppNameLabelKey:      "dragonfly",
				KubernetesAppVersionLabelKey:   Version,
				KubernetesPartOfLabelKey:       "dragonfly",
				KubernetesManagedByLabelKey:    DragonflyOperatorName,
				"app":                          db.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"app":                     db.Name,
				KubernetesAppNameLabelKey: "dragonfly",
				"role":                    "master",
			},
			Ports: []corev1.ServicePort{
				{
					Name: DragonflyPortName,
					Port: DragonflyPort,
				},
			},
		},
	}

	resources = append(resources, &service)

	return resources, nil
}
