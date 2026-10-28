{{- /* What every AppDependencies admission policy shares. */ -}}

{{- define "cloudctl-operator.policyLabels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/name: {{ include "cloudctl-operator.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "cloudctl-operator.policyActions" -}}
{{- if eq .Values.policies.action "Deny" -}}
[Deny, Audit]
{{- else if eq .Values.policies.action "Warn" -}}
[Warn, Audit]
{{- else if eq .Values.policies.action "Audit" -}}
[Audit]
{{- else -}}
{{- fail (printf "policies.action must be Deny, Warn or Audit, not %q" .Values.policies.action) -}}
{{- end -}}
{{- end }}

{{- /*
  A request is judged only when it creates a CR or changes its spec. The
  operator adds and removes its finalizer with a metadata-only update, so that
  update is never refused: a CR in a namespace that has lost its label, or one
  the limits have since been tightened below, can still be deleted.
*/ -}}
{{- define "cloudctl-operator.policyMatch" -}}
matchConstraints:
  resourceRules:
    - apiGroups: ["deps.cloudctl.io"]
      apiVersions: ["v1alpha1"]
      operations: ["CREATE", "UPDATE"]
      resources: ["appdependencies"]
matchConditions:
  - name: creates-and-spec-changes
    expression: "request.operation == 'CREATE' || object.spec != oldObject.spec"
{{- end }}

{{- /* One variable per resource kind: its declared entries, or none. */ -}}
{{- define "cloudctl-operator.policyVariables" -}}
{{- range $kind := list "sqs" "sns" "s3" "dynamodb" "rds" "kms" }}
- name: {{ $kind }}
  expression: "has(object.spec.{{ $kind }}) && has(object.spec.{{ $kind }}.resources) ? object.spec.{{ $kind }}.resources : []"
{{- end }}
{{- end }}
