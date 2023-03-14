// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package resources

const (
	// DragonflyPort is the port on which the database listens
	DragonflyPort = 6379

	// DragonflyPortName is the name of the port on which the database listens
	DragonflyPortName = "database"

	// DragonflyOperatorName is the name of the operator
	DragonflyOperatorName = "dragonfly-operator"

	// DragonflyImage is the default image of the database to use
	// TODO: Change this
	DragonflyImage = "docker.dragonflydb.io/dragonflydb/dragonfly"

	// DragonflyHealthCheckPath is the path on which the database exposes its health check
	DragonflyHealthCheckPath = "/health"

	// Recommended Kubernetes Application Labels
	// KubernetesAppNameLabel is the name of the application
	KubernetesAppNameLabelKey = "app.kubernetes.io/name"

	// KubernetesAppVersionLabel is the version of the application
	KubernetesAppVersionLabelKey = "app.kubernetes.io/version"

	// KubernetesAppComponentLabel is the component of the application
	KubernetesAppComponentLabelKey = "app.kubernetes.io/component"

	KubernetesAppInstanceNameLabel = "app.kubernetes.io/instance"

	// KubernetesManagedByLabel is the tool being used to manage the operation of an application
	KubernetesManagedByLabelKey = "app.kubernetes.io/managed-by"

	// KubernetesPartOfLabel is the name of a higher level application this one is part of
	KubernetesPartOfLabelKey = "app.kubernetes.io/part-of"
)
