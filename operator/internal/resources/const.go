// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

package resources

const (
	// DragonflyDbPort is the port on which the database listens
	DragonflyDbPort = 6379

	// DragonflyDbPortName is the name of the port on which the database listens
	DragonflyDbPortName = "database"

	// DragonflyDbOperatorName is the name of the operator
	DragonflyDbOperatorName = "dragonflydb-operator"

	// DragonflyDbImage is the default image of the database to use
	DragonflyDbImage = "docker.dragonflydb.io/dragonflydb/dragonfly"

	// DragonflyDbHealthCheckPath is the path on which the database exposes its health check
	DragonflyDbHealthCheckPath = "/health"

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
