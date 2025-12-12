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

{{- define "common-app.input-env" -}}
{{- if and (hasKey .Values.kafka "inputTopics") (.Values.kafka.inputTopics) }}
- name: "{{ .Values.configurationEnvPrefix }}_INPUT_TOPICS"
  value: {{ .Values.kafka.inputTopics | join "," | quote }}
{{- end }}
{{- if hasKey .Values.kafka "inputPattern" }}
- name: "{{ .Values.configurationEnvPrefix }}_INPUT_PATTERN"
  value: {{ .Values.kafka.inputPattern | quote }}
{{- end }}
{{- $delimiter := ";" }}
{{- if and (hasKey .Values.kafka "labeledInputTopics") (.Values.kafka.labeledInputTopics) }}
- name: "{{ .Values.configurationEnvPrefix }}_LABELED_INPUT_TOPICS"
  value: "{{- range $key, $value := .Values.kafka.labeledInputTopics }}{{ $key }}={{ $value | join $delimiter }},{{- end }}"
{{- end }}
{{- if and (hasKey .Values.kafka "labeledInputPatterns") (.Values.kafka.labeledInputPatterns) }}
- name: "{{ .Values.configurationEnvPrefix }}_LABELED_INPUT_PATTERNS"
  value: "{{- range $key, $value := .Values.kafka.labeledInputPatterns }}{{ $key }}={{ $value }},{{- end }}"
{{- end }}
{{- end }}

{{- define "common-app.output-env" -}}
{{- if hasKey .Values.kafka "outputTopic" }}
- name: "{{ .Values.configurationEnvPrefix }}_OUTPUT_TOPIC"
  value: {{ .Values.kafka.outputTopic | quote }}
{{- end }}
{{- if and (hasKey .Values.kafka "labeledOutputTopics") (.Values.kafka.labeledOutputTopics) }}
- name: "{{ .Values.configurationEnvPrefix }}_LABELED_OUTPUT_TOPICS"
  value: "{{- range $key, $value := .Values.kafka.labeledOutputTopics }}{{ $key }}={{ $value }},{{- end }}"
{{- end }}
{{- end }}

{{- define "common-app.error-env" -}}
{{- if hasKey .Values.kafka "errorTopic" }}
- name: "{{ .Values.configurationEnvPrefix }}_ERROR_TOPIC"
  value: {{ .Values.kafka.errorTopic | quote }}
{{- end }}
{{- end }}

{{- define "common-app.group-instance-id-env" -}}
{{- if .Values.kafka.staticMembership }}
- name: KAFKA_GROUP_INSTANCE_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- end }}
{{- if not .Values.statefulSet }}
- name: "{{ .Values.configurationEnvPrefix }}_VOLATILE_GROUP_INSTANCE_ID"
  value: "true"
{{- end }}
{{- end }}

{{- define "common-app.group-id-env" -}}
{{- if hasKey .Values.kafka "groupId" }}
- name: "{{ .Values.configurationEnvPrefix }}_GROUP_ID"
  value: {{ .Values.kafka.groupId | quote }}
{{- end }}
{{- end }}

{{- define "common-app.application-id-env" -}}
{{- if hasKey .Values.kafka "applicationId" }}
- name: "{{ .Values.configurationEnvPrefix }}_APPLICATION_ID"
  value: {{ .Values.kafka.applicationId | quote }}
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

{{- define "common-app.jmx-volume" -}}
{{- if .Values.prometheus.jmx.enabled }}
- name: jmx-config
  configMap:
    name: {{ include "common-app.fullname" . }}-jmx
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

{{- define "common-app.common-pod-spec" -}}
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
  {{- if .Values.imagePullSecrets }}
  imagePullSecrets:
{{- toYaml .Values.imagePullSecrets | nindent 4 }}
  {{- end }}
{{- end }}

{{- define "common-app.cleanup-pod-spec" -}}
{{- include "common-app.common-pod-spec" . }}
  restartPolicy: {{ .Values.restartPolicy }}
  {{- if or (.Values.files) (.Values.secretFilesRefs) }}
  volumes:
    {{- include "common-app.volumes" . | nindent 8 }}
  {{- end }}
{{- end }}

{{- define "common-app.pod-spec" -}}
{{- include "common-app.common-pod-spec" . }}
  {{- if .Values.priorityClassName }}
  priorityClassName: {{ .Values.priorityClassName }}
  {{- end }}
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
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

{{- define "common-app.cleanup-pod-metadata" -}}
{{- if .Values.podAnnotations }}
  annotations:
  {{- range $key, $value := .Values.podAnnotations }}
    {{ $key | quote }}: {{ $value | quote }}
  {{- end }}
{{- end }}
  labels:
    {{- include "common-app.selectorLabels" . | nindent 4 }}
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

{{- define "common-app.java-tool-jmx-options" -}}
-Dcom.sun.management.jmxremote.port={{ .Values.jmx.port }}
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
{{- if .Values.jmx.enabled }}
-Djava.rmi.server.hostname={{ .Values.jmx.host }}
-Dcom.sun.management.jmxremote.rmi.port={{ .Values.jmx.port }}
{{- end }}
{{- end }}

{{- define "common-app.cleanup-job-spec" -}}
ttlSecondsAfterFinished: 30
backoffLimit: {{ .Values.backoffLimit }}
{{- end }}

{{- define "common-app.deployment-spec" -}}
  {{- if .Values.statefulSet }}
  serviceName: {{ include "common-app.fullname" . }}
  podManagementPolicy: Parallel
  {{- end }}
  {{- if (not .Values.autoscaling.enabled) }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "common-app.selectorLabels" . | nindent 6 }}
  {{- if and .Values.persistence.enabled .Values.statefulSet }}
  volumeClaimTemplates:
    - metadata:
        name: datadir
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: "{{ .Values.persistence.size }}"
        {{- if .Values.persistence.storageClass }}
        {{- if (eq "-" .Values.persistence.storageClass) }}
        storageClassName: ""
        {{- else }}
        storageClassName: "{{ .Values.persistence.storageClass }}"
        {{- end }}
        {{- end }}
  {{- end }}
{{- end }}

{{- define "common-app.deployment" -}}
{{- if .Capabilities.APIVersions.Has "apps/v1" }}
apiVersion: apps/v1
{{- else }}
apiVersion: apps/v1beta1
{{- end }}
{{- if .Values.statefulSet }}
kind: StatefulSet
{{- else }}
kind: Deployment
{{- end }}
metadata:
  name: {{ include "common-app.fullname" . }}
  {{- include "common-app.deployment-annotations" . }}
  labels:
    {{- include "common-app.labels" . | nindent 4 }}
    streams-bootstrap/kind: {{ .Chart.Name }}
    {{- range $key, $value := .Values.labels }}
    {{ $key | quote }}: {{ $value | quote }}
    {{- end }}
{{- end }}

{{- define "common-app.cleanup-job" -}}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "common-app.fullname" . }}
  {{- include "common-app.annotations" . }}
  labels:
    {{- include "common-app.labels" . | nindent 4 }}
    {{- range $key, $value := .Values.labels }}
    {{ $key | quote }}: {{ $value | quote }}
    {{- end }}
{{- end }}
