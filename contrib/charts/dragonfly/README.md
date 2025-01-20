# dragonfly

![Version: v0.12.0](https://img.shields.io/badge/Version-v0.12.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v0.12.0](https://img.shields.io/badge/AppVersion-v0.12.0-informational?style=flat-square)

Dragonfly is a modern in-memory datastore, fully compatible with Redis and Memcached APIs.

**Homepage:** <https://dragonflydb.io/>

## Source Code

* <https://github.com/dragonflydb/dragonfly>

## Requirements

Kubernetes: `>=1.23.0-0`


## Installing from a pre-packaged OCI

Pick a version from https://github.com/dragonflydb/dragonfly/pkgs/container/dragonfly%2Fhelm%2Fdragonfly

Example:

```shell
VERSION=v1.12.1
helm upgrade --install dragonfly oci://ghcr.io/dragonflydb/dragonfly/helm/dragonfly --version $VERSION
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity for pod assignment |
| command | list | `[]` | Allow overriding the container's command |
| extraArgs | list | `[]` | Extra arguments to pass to the dragonfly binary |
| extraContainers | list | `[]` | Additional sidecar containers |
| extraObjects | list | `[]` | extra K8s manifests to deploy |
| extraVolumeMounts | list | `[]` | Extra volume mounts corresponding to the volumes mounted above |
| extraVolumes | list | `[]` | Extra volumes to mount into the pods |
| fullnameOverride | string | `""` | String to fully override dragonfly.fullname |
| image.pullPolicy | string | `"IfNotPresent"` | Dragonfly image pull policy |
| image.repository | string | `"docker.dragonflydb.io/dragonflydb/dragonfly"` | Container Image Registry to pull the image from |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` | Container Registry Secret names in an array |
| initContainers | list | `[]` | A list of initContainers to run before each pod starts |
| nameOverride | string | `""` | String to partially override dragonfly.fullname |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Annotations for pods |
| podSecurityContext | object | `{}` | Set securityContext for pod itself |
| probes.livenessProbe.exec.command[0] | string | `"/bin/sh"` |  |
| probes.livenessProbe.exec.command[1] | string | `"/usr/local/bin/healthcheck.sh"` |  |
| probes.livenessProbe.failureThreshold | int | `3` |  |
| probes.livenessProbe.initialDelaySeconds | int | `10` |  |
| probes.livenessProbe.periodSeconds | int | `10` |  |
| probes.livenessProbe.successThreshold | int | `1` |  |
| probes.livenessProbe.timeoutSeconds | int | `5` |  |
| probes.readinessProbe.exec.command[0] | string | `"/bin/sh"` |  |
| probes.readinessProbe.exec.command[1] | string | `"/usr/local/bin/healthcheck.sh"` |  |
| probes.readinessProbe.failureThreshold | int | `3` |  |
| probes.readinessProbe.initialDelaySeconds | int | `10` |  |
| probes.readinessProbe.periodSeconds | int | `10` |  |
| probes.readinessProbe.successThreshold | int | `1` |  |
| probes.readinessProbe.timeoutSeconds | int | `5` |  |
| prometheusRule.enabled | bool | `false` | Deploy a PrometheusRule |
| prometheusRule.spec | list | `[]` | PrometheusRule.Spec https://awesome-prometheus-alerts.grep.to/rules |
| replicaCount | int | `1` | Number of replicas to deploy |
| resources.limits | object | `{}` | The resource limits for the containers |
| resources.requests | object | `{}` | The requested resources for the containers |
| env | list | `[]` | Extra environment variables |
| envFrom | list | `[]` | Extra environment variables from K8s objects |
| securityContext | object | `{}` | Set securityContext for containers |
| service.annotations | object | `{}` | Extra annotations for the service |
| service.lablels | object | `{}` | Extra labels for the service |
| service.metrics.portName | string | `"metrics"` | name for the metrics port |
| service.metrics.serviceType | string | `"ClusterIP"` | serviceType for the metrics service |
| service.port | int | `6379` | Dragonfly service port |
| service.type | string | `"ClusterIP"` | Service type to provision. Can be NodePort, ClusterIP or LoadBalancer |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| serviceMonitor.annotations | object | `{}` | additional annotations to apply to the metrics |
| serviceMonitor.enabled | bool | `false` | If true, a ServiceMonitor CRD is created for a prometheus operator |
| serviceMonitor.interval | string | `"10s"` | scrape interval |
| serviceMonitor.labels | object | `{}` | additional labels to apply to the metrics |
| serviceMonitor.namespace | string | `""` | namespace in which to deploy the ServiceMonitor CR. defaults to the application namespace |
| serviceMonitor.scrapeTimeout | string | `"10s"` | scrape timeout |
| storage.enabled | bool | `false` | If /data should persist. This will provision a StatefulSet instead. |
| storage.requests | string | `"128Mi"` | Volume size to request for the PVC |
| storage.storageClassName | string | `""` | Global StorageClass for Persistent Volume(s) |
| tls.cert | string | `""` | TLS certificate |
| tls.createCerts | bool | `false` | use cert-manager to automatically create the certificate |
| tls.duration | string | `"87600h0m0s"` | duration or ttl of the validity of the created certificate |
| tls.enabled | bool | `false` | enable TLS |
| tls.existing_secret | string | `""` | use TLS certificates from existing secret |
| tls.issuer.kind | string | `"ClusterIssuer"` | cert-manager issuer kind. Usually Issuer or ClusterIssuer |
| tls.issuer.name | string | `"selfsigned"` | name of the referenced issuer |
| tls.key | string | `""` | TLS private key |
| tolerations | list | `[]` | Tolerations for pod assignment |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.11.0](https://github.com/norwoodj/helm-docs/releases/v1.11.0)
