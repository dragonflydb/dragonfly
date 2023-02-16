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
    {{- toYaml . | nindent 2 }}
  {{- end }}
{{- end }}
{{- end }}

{{- define "dragonfly.pod" -}}
serviceAccountName: {{ include "dragonfly.serviceAccountName" . }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.podSecurityContext }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.initContainers }}
initContainers:
  {{- if eq (typeOf .) "string" }}
  {{- tpl . $ | nindent 2 }}
  {{- else }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- end }}
containers:
  - name: {{ .Chart.Name }}
    {{- with .Values.securityContext }}
    securityContext:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
    imagePullPolicy: {{ .Values.image.pullPolicy }}
    ports:
      - name: dragonfly
        containerPort: 6379
        protocol: TCP
    {{- with .Values.probes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
    {{- with .Values.command }}
    command:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    args:
      - "--alsologtostderr"
    {{- with .Values.extraArgs }}
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- if .Values.tls.enabled }}
      - "--tls"
      - "--tls_cert_file=/etc/dragonfly/tls/tls.crt"
      - "--tls_key_file=/etc/dragonfly/tls/tls.key"
    {{- end }}
    {{- with .Values.resources }}
    resources:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- include "dragonfly.volumemounts" . | nindent 4 }}
    env:
    {{- if .Values.passwordFromSecret.enable }}
      - name: DFLY_PASSWORD
        valueFrom:
          secretKeyRef:
            name: {{ .Values.passwordFromSecret.existingSecret.name }}
            key: {{ .Values.passwordFromSecret.existingSecret.key }}
    {{- end }}
  {{- with .Values.extraContainers }}
  {{- if eq (typeOf .) "string" }}
  {{- tpl . $ | nindent 2 }}
  {{- else }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
  {{- end }}

{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
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
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
