{{/*
Expand the name of the chart.
*/}}
{{- define "dragonfly.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dragonfly.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
StatefulSet serviceName. Defaults to the release name, matching this chart's
historical behavior so existing storage-enabled releases are unaffected on
upgrade (Kubernetes does not allow changing serviceName in place). Set
storage.useFullnameForVolumes to base it on the fullname instead, which
avoids name collisions when this chart is installed as a subchart dependency.
*/}}
{{- define "dragonfly.serviceName" -}}
{{- if .Values.storage.useFullnameForVolumes }}
{{- include "dragonfly.fullname" . }}
{{- else }}
{{- .Release.Name }}
{{- end }}
{{- end }}

{{/*
Data volume / PVC name. Defaults to "<release-name>-data", matching this
chart's historical behavior so existing storage-enabled releases are
unaffected on upgrade (Kubernetes does not allow renaming
volumeClaimTemplates in place). Set storage.useFullnameForVolumes to base it
on the fullname instead, which avoids name collisions when this chart is
installed as a subchart dependency. Truncates the combined string (rather
than truncating fullname alone and appending "-data") to respect the 63 char
Kubernetes name limit.
*/}}
{{- define "dragonfly.dataVolumeName" -}}
{{- if .Values.storage.useFullnameForVolumes }}
{{- printf "%s-data" (include "dragonfly.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-data" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dragonfly.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dragonfly.labels" -}}
helm.sh/chart: {{ include "dragonfly.chart" . }}
{{ include "dragonfly.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- include "dragonfly.commonLabels" . }}
{{- end }}

{{/*
User-defined common labels
*/}}
{{- define "dragonfly.commonLabels" -}}
{{- if .Values.commonLabels }}
{{- range $key, $value := .Values.commonLabels }}
{{ $key }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dragonfly.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dragonfly.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "dragonfly.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dragonfly.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
