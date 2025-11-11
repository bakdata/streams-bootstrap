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

