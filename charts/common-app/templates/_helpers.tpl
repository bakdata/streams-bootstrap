{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "common-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "common-app.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "common-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "common-app.labels" -}}
helm.sh/chart: {{ include "common-app.chart" . }}
{{ include "common-app.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "common-app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "common-app.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define default annotations from .Values.annotations.
This will be used across resources.
*/}}
{{- define "common-app.annotations" -}}
{{- if or .Values.annotations }}
  annotations:
{{- range $key, $value := .Values.annotations }}
    {{ $key | quote }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Define annotations helper for Deployment.
Includes default annotations and conditionally adds consumerGroup if applicable.
*/}}
{{- define "common-app.deployment-annotations" -}}
{{/* Use applicationId for Kafka Streams, otherwise use groupId for Kafka Consumers */}}
{{- $uniqueId := coalesce .Values.kafka.applicationId .Values.kafka.groupId }}
{{- if or .Values.annotations $uniqueId }}
  annotations:
{{- range $key, $value := .Values.annotations }}
    {{ $key | quote }}: {{ $value | quote }}
{{- end }}

  {{- /* Conditionally add the consumerGroup annotation if needed */ -}}
  {{- if and $uniqueId (not .Values.annotations.consumerGroup) }}
    consumerGroup: {{ $uniqueId | quote }}
  {{- end }}
{{- end }}
{{- end }}

{{- define "common-app.common-env" -}}
{{- $root := . -}}
- name: ENV_PREFIX
  value: {{ .Values.configurationEnvPrefix }}_
{{- range $key, $value := .Values.kafka.config }}
- name: {{ printf "KAFKA_%s" $key | replace "." "_" | upper | quote }}
  value: {{ $value | quote }}
{{- end }}
{{- if hasKey .Values.kafka "bootstrapServers" }}
- name: "{{ .Values.configurationEnvPrefix }}_BOOTSTRAP_SERVERS"
  value: {{ .Values.kafka.bootstrapServers | quote }}
{{- end }}
{{- if hasKey .Values.kafka "schemaRegistryUrl" }}
- name: "{{ .Values.configurationEnvPrefix }}_SCHEMA_REGISTRY_URL"
  value: {{ .Values.kafka.schemaRegistryUrl | quote }}
{{- end }}
{{- range $key, $value := .Values.secrets }}
- name: "{{ $key }}"
  valueFrom:
    secretKeyRef:
      name: {{ include "common-app.fullname" . }}
      key: "{{ $key }}"
{{- end }}
{{- range $key, $value := .Values.secretRefs }}
- name: "{{ $key }}"
  valueFrom:
    secretKeyRef:
      name: {{ $value.name }}
      key: "{{ $value.key }}"
{{- end }}
{{- range $key, $value := .Values.commandLine }}
- name: "{{ $root.Values.configurationEnvPrefix }}_{{ $key }}"
  value: {{ $value | quote }}
{{- end }}
{{- range $key, $value := .Values.env }}
- name: {{ $key | quote }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}

{{- define "common-app.volume-mounts" -}}
{{- range $key, $value := .Values.files }}
- name: config
  mountPath: {{ printf "%s/%s" $value.mountPath $key | quote }}
  subPath: {{ $key | quote }}
{{- end }}
{{- range .Values.secretFilesRefs }}
- name: {{ .volume }}
  mountPath: {{ .mountPath }}
  {{- if .readOnly }}
  readOnly: true
  {{- end }}
  {{- if .subPath}}
  subPath: {{.subPath }}
  {{- end }}
{{- end }}
{{- end }}

{{- define "common-app.volumes" -}}
{{- if .Values.files }}
- name: config
  configMap:
    name: {{ include "common-app.fullname" . }}
{{- end }}
{{- range .Values.secretFilesRefs }}
- name: {{ .volume }}
  secret:
    secretName: {{ .name }}
{{- end }}
{{- end }}

{{- define "common-app.ports" -}}
{{- range .Values.ports }}
- containerPort: {{ .containerPort }}
  name: {{ .name | quote }}
  {{- if .protocol }}
  protocol: {{ .protocol | quote }}
  {{- end }}
{{- end }}
{{- end }}

{{- define "common-app.pod-spec" -}}
{{- $root := . -}}
  {{- if .Values.serviceAccountName }}
  serviceAccountName: {{ .Values.serviceAccountName }}
  {{- end }}
  {{- if .Values.tolerations }}
  tolerations:
{{ toYaml .Values.tolerations | indent 4 }}
  {{- end }}
  {{- with .Values.affinity }}
  affinity:
    {{- tpl (toYaml .) $root | nindent 4 }}
  {{- end }}
  {{- if .Values.priorityClassName }}
  priorityClassName: {{ .Values.priorityClassName }}
  {{- end }}
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
  {{- if .Values.imagePullSecrets }}
  imagePullSecrets:
{{- toYaml .Values.imagePullSecrets | nindent 4 }}
  {{- end }}
{{- end }}

{{- define "common-app.pod-metadata" -}}
{{- if or .Values.podAnnotations .Values.files }}
  annotations:
  {{- if .Values.files }}
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
  {{- end }}
  {{- range $key, $value := .Values.podAnnotations }}
    {{ $key | quote }}: {{ $value | quote }}
  {{- end }}
{{- end }}
  labels:
    {{- include "common-app.selectorLabels" . | nindent 4 }}
    streams-bootstrap/kind: {{ .Chart.Name }}
    {{- range $key, $value := .Values.podLabels }}
    {{ $key }}: {{ $value }}
    {{- end }}
{{- end }}

{{- define "common-app.common-kafka-container" -}}
- name: "kafka-app"
  image: "{{ .Values.image }}:{{ .Values.imageTag }}"
  imagePullPolicy: "{{ .Values.imagePullPolicy }}"
  resources:
{{ toYaml .Values.resources | indent 4 }}
  {{- if .Values.livenessProbe }}
  livenessProbe:
{{- .Values.livenessProbe | toYaml | nindent 4 }}
  {{- end }}
  {{- if .Values.readinessProbe }}
  readinessProbe:
{{- .Values.readinessProbe | toYaml | nindent 4 }}
  {{- end }}
{{- end }}

{{- define "common-app.java-tool-options" -}}
-XX:MaxRAMPercentage={{ printf "%.1f" .Values.javaOptions.maxRAMPercentage }}
{{ .Values.javaOptions.others | join " " }}
{{- end }}
