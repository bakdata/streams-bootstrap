{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "consumerproducer-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "consumerproducer-app.fullname" -}}
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
{{- define "consumerproducer-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "consumerproducer-app.labels" -}}
helm.sh/chart: {{ include "consumerproducer-app.chart" . }}
{{ include "consumerproducer-app.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "consumerproducer-app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "consumerproducer-app.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define default annotations from .Values.annotations.
This will be used across resources.
*/}}
{{- define "consumerproducer-app.annotations" -}}
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
{{- define "consumerproducer-app.deployment-annotations" -}}
{{- if or .Values.annotations .Values.kafka.groupId }}
  annotations:
{{- range $key, $value := .Values.annotations }}
    {{ $key | quote }}: {{ $value | quote }}
{{- end }}

  {{- /* Conditionally add the consumerGroup annotation if needed */ -}}
  {{- if and .Values.kafka.groupId (not .Values.annotations.consumerGroup) }}
    consumerGroup: {{ .Values.kafka.groupId | quote }}
  {{- end }}
{{- end }}
{{- end }}

