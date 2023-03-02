// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DragonflyDbSpec defines the desired state of DragonflyDb
type DragonflyDbSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Replicas is the number of replicas of the database
	Replicas int32 `json:"replicas,omitempty"`
	// Image is the image of the database to use
	Image string `json:"image,omitempty"`

	// (Optional) Database container resource limits. Any container limits
	// can be specified.
	// Default: (not specified)
	// +optional
	// +kubebuilder:validation:Optional
	// Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// DragonflyDbStatus defines the observed state of DragonflyDb
type DragonflyDbStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Created is true if the database resources have been created
	Created bool `json:"created,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// DragonflyDb is the Schema for the dragonflydbs API
type DragonflyDb struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DragonflyDbSpec   `json:"spec,omitempty"`
	Status DragonflyDbStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DragonflyDbList contains a list of DragonflyDb
type DragonflyDbList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DragonflyDb `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DragonflyDb{}, &DragonflyDbList{})
}
