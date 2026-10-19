The Tekton plugin connects Kestra to Tekton Pipelines installed on Kubernetes.

## Authentication

Tasks and triggers accept a `connection` object. Fabric8 can use the current kubeconfig or in-cluster service account automatically. For explicit authentication, provide `masterUrl`, `oauthToken`, and optionally `caCertData`. Secret values should be written as Kestra secret expressions, for example `{{ secret('K8S_TOKEN') }}`.

## PipelineRuns

Use `pipelinerun.Create` to create a run from `pipelineRef` or an inline `spec`, with `params`, `workspaces`, and `serviceAccountName`. Set `wait: true` to poll the Tekton `Succeeded` condition. A `False` condition fails the Kestra task; `timeout` limits how long it waits. `Get`, `List`, and `Cancel` read, filter, and cancel runs.

## TaskRuns

Use `taskrun.Create` and `taskrun.Get` for single Tekton Tasks. `Create` supports `taskRef`, inline `spec`, parameters, workspace bindings, optional waiting, timeout, and cancellation when the Kestra execution is killed.

## Trigger

`pipelinerun.Trigger` polls terminal PipelineRuns selected by `labelSelector`. It stores the last emitted run name and completion time in the namespace KV Store, so repeated polls do not emit the same run twice. The trigger exposes the run name, namespace, status, and completion time to the execution.

## Cluster requirements

The target cluster must expose the Tekton `tekton.dev/v1` API. The plugin does not install Tekton or create Kubernetes namespaces.
