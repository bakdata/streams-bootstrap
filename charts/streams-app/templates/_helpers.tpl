{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "streams-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "streams-app.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Values.nameOverride -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "streams-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Define default annotations from .Values.annotations.
This will be used across resources.
*/}}
{{- define "streams-app.default-annotations" -}}
{{- if or .Values.annotations }}
  annotations:
{{- range $key, $value := .Values.annotations }}
    {{ $key | quote }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Define annotations helper for Deployment.
Includes default annotations and conditionally adds consumerGroup.
Only includes annotations if there is content.
*/}}
{{- define "streams-app.deployment-annotations" -}}
{{- include "streams-app.default-annotations" . }}
{{- if .Values.kafka.applicationId }}
  {{- if and .Values.kafka.applicationId (not .Values.annotations.consumerGroup) }}
    consumerGroup: {{ .Values.kafka.applicationId | quote }}
  {{- end }}
{{- end }}
{{- end }}

