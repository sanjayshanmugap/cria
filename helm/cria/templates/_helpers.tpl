{{- define "cria.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name -}}
{{- end -}}

{{- define "cria.modelRoutes" -}}
{{- $routes := list -}}
{{- range .Values.models -}}
{{- $routes = append $routes (printf "%s=%s" .id .topic) -}}
{{- end -}}
{{- join "," $routes -}}
{{- end -}}

{{- define "cria.topics" -}}
{{- $topics := list "inference_token_events" "inference_control_events" -}}
{{- range .Values.models -}}
{{- $topics = append $topics .topic -}}
{{- end -}}
{{- join " " $topics -}}
{{- end -}}

{{- define "cria.sanitizeModelId" -}}
{{- . | lower | replace "." "-" | replace "_" "-" -}}
{{- end -}}
