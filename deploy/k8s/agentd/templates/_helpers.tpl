{{- define "agentd.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentd.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "agentd.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "agentd.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "agentd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "agentd.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agentd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: control-plane
{{- end }}

{{- define "agentd.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "agentd.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "agentd.databaseSecretName" -}}
{{ include "agentd.fullname" . }}-database
{{- end }}

{{- define "agentd.authSecretName" -}}
{{- default (printf "%s-auth" (include "agentd.fullname" .)) .Values.auth.existingSecret -}}
{{- end }}

{{- define "agentd.mysqlName" -}}
{{ include "agentd.fullname" . }}-mysql
{{- end }}

{{- define "agentd.databaseDSN" -}}
{{- if .Values.database.dsn -}}
{{ .Values.database.dsn }}
{{- else -}}
{{- $auth := .Values.database.mysql.auth -}}
{{ printf "%s:%s@tcp(%s:3306)/%s" $auth.username $auth.password (include "agentd.mysqlName" .) $auth.database }}
{{- end -}}
{{- end }}
