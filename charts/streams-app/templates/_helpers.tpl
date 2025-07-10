{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "streams-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "streams-app.fullname" -}}
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
{{- define "streams-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "streams-app.labels" -}}
helm.sh/chart: {{ include "streams-app.chart" . }}
{{ include "streams-app.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "streams-app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "streams-app.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define default annotations from .Values.annotations.
This will be used across resources.
*/}}
{{- define "streams-app.annotations" -}}
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
{{- define "streams-app.deployment-annotations" -}}
{{- if or .Values.annotations .Values.kafka.applicationId }}
  annotations:
{{- range $key, $value := .Values.annotations }}
    {{ $key | quote }}: {{ $value | quote }}
{{- end }}

  {{- /* Conditionally add the consumerGroup annotation if needed */ -}}
  {{- if and .Values.kafka.applicationId (not .Values.annotations.consumerGroup) }}
    consumerGroup: {{ .Values.kafka.applicationId | quote }}
  {{- end }}
{{- end }}
{{- end }}

