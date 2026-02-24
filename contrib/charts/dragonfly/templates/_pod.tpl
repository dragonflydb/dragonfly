{{- define "dragonfly.volumemounts" -}}
{{- if or (.Values.storage.enabled) (.Values.extraVolumeMounts) (.Values.tls.enabled) }}
volumeMounts:
  {{- if .Values.storage.enabled }}
  - mountPath: /data
    name: "{{ .Release.Name }}-data"
  {{- end }}
  {{- if and .Values.tls .Values.tls.enabled }}
  - mountPath: /etc/dragonfly/tls
    name: tls
  {{- end }}
  {{- with .Values.extraVolumeMounts }}
    {{- toYaml . | trim | nindent 2 }}
  {{- end }}
{{- end }}
{{- end }}

{{- define "dragonfly.pod" -}}
{{- if ne .Values.priorityClassName "" }}
priorityClassName: {{ .Values.priorityClassName }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | trim | nindent 2 -}}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | trim | nindent 2 -}}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | trim | nindent 2 -}}
{{- end }}
serviceAccountName: {{ include "dragonfly.serviceAccountName" . }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | trim | nindent 2 }}
{{- end }}
{{- with .Values.podSecurityContext }}
securityContext:
  {{- toYaml . | trim | nindent 2 }}
{{- end }}
{{- if and (eq (typeOf .Values.hostNetwork) "bool") .Values.hostNetwork }}
hostNetwork: true
{{- end }}
{{- with .Values.topologySpreadConstraints }}
topologySpreadConstraints:
  {{- toYaml . | trim | nindent 2 }}
{{- end }}
{{- with .Values.initContainers }}
initContainers:
  {{- if eq (typeOf .) "string" }}
  {{- tpl . $ | trim | nindent 2 }}
  {{- else }}
  {{- toYaml . | trim | nindent 2 }}
  {{- end }}
{{- end }}
containers:
  {{- with .Values.extraContainers }}
  {{- if eq (typeOf .) "string" -}}
  {{- tpl . $ | trim | nindent 2 }}
  {{- else }}
  {{- toYaml . | trim | nindent 2 }}
  {{- end }}
  {{- end }}
  - name: {{ .Chart.Name }}
    {{- with .Values.securityContext }}
    securityContext:
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
    image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
    imagePullPolicy: {{ .Values.image.pullPolicy }}
    ports:
      - name: dragonfly
        containerPort: 6379
        protocol: TCP
    {{- with .Values.probes }}
    {{- toYaml . | trim | nindent 4 }}
    {{- end }}
    {{- with .Values.command }}
    command:
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
    args:
      - "--alsologtostderr"
      {{- if .Values.snapshot.enabled }}
      - "--snapshot_cron"
      - "{{ .Values.snapshot.schedule }}"
      {{- if eq .Values.snapshot.format "rdb" }}
      - "--nodf_snapshot_format"
      {{- end }}
      {{- end }}
    {{- with .Values.extraArgs }}
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
    {{- if .Values.tls.enabled }}
      - "--tls"
      - "--tls_cert_file=/etc/dragonfly/tls/tls.crt"
      - "--tls_key_file=/etc/dragonfly/tls/tls.key"
    {{- end }}
    {{- with .Values.resources }}
    resources:
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
    {{- include "dragonfly.volumemounts" . | trim | nindent 4 }}
    {{- if or .Values.passwordFromSecret.enable .Values.env }}
    env:
    {{- if .Values.passwordFromSecret.enable }}
    {{- $appVersion := .Chart.AppVersion | trimPrefix "v" }}
    {{- $imageTag := .Values.image.tag | trimPrefix "v" }}
    {{- $effectiveVersion := $appVersion }}
    {{- if and $imageTag (ne $imageTag "") }}
      {{- $effectiveVersion = $imageTag }}
    {{- end }}
    {{- if semverCompare ">=1.14.0" $effectiveVersion }}
      - name: DFLY_requirepass
    {{- else }}
      - name: DFLY_PASSWORD
    {{- end }}
        valueFrom:
          secretKeyRef:
            name: {{ tpl .Values.passwordFromSecret.existingSecret.name $ }}
            key: {{ .Values.passwordFromSecret.existingSecret.key }}
    {{- end }}
    {{- with .Values.env }}
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
    {{- end }}
    {{- with .Values.envFrom }}
    envFrom:
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
  {{- if .Values.snapshot.enabled }}
  - name: snapshot-cleaner
    image: {{ .Values.snapshot.cleaner.image }}
    command:
      - /bin/sh
      - -c
      - |
        while true; do
          {{- if eq .Values.snapshot.format "dragonfly" }}
          # Keep only the last {{ .Values.snapshot.cleaner.max_count }} snapshots (composed of two files each)
          ls -1t /data/dump-*.dfs | tail -n +{{ add 1 (mul .Values.snapshot.cleaner.max_count 2) }} | xargs rm -f
          {{- else }}
          # Keep only the last {{ .Values.snapshot.cleaner.max_count }} snapshots
          ls -1t /data/dump-*.rdb | tail -n +{{ add 1 .Values.snapshot.cleaner.max_count }} | xargs rm -f
          {{- end }}
          sleep {{ .Values.snapshot.cleaner.interval }}
        done
    volumeMounts:
      - mountPath: /data
        name: "{{ .Release.Name }}-data"
    {{- with .Values.snapshot.cleaner.resources }}
    resources:
      {{- toYaml . | trim | nindent 6 }}
    {{- end }}
  {{- end }}

{{- if or (.Values.tls.enabled) (.Values.extraVolumes) }}
volumes:
{{- if and .Values.tls .Values.tls.enabled }}
  {{- if .Values.tls.existing_secret }}
  - name: tls
    secret:
      secretName: {{ .Values.tls.existing_secret }}
  {{- else if .Values.tls.createCerts }}
  - name: tls
    secret:
      secretName: '{{ include "dragonfly.fullname" . }}-server-tls'
  {{- else }}
  - name: tls
    secret:
      secretName: {{ include "dragonfly.fullname" . }}-tls
  {{- end }}
{{- end }}
{{- with .Values.extraVolumes }}
  {{- toYaml . | trim | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
