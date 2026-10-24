# How to use the Tekton plugin

Connect Kestra to Tekton Pipelines installed on Kubernetes and orchestrate PipelineRuns and TaskRuns from a flow.

See the [plugin defaults](https://kestra.io/docs/plugin-developer-guide/plugin-defaults) for shared task configuration and default behavior.

## Authentication

Tasks and triggers accept a `connection` object. Fabric8 can use the current kubeconfig or in-cluster service account automatically. For explicit authentication, provide `masterUrl`, `oauthToken`, and optionally `caCertData`. Store credentials in [Kestra secrets](https://kestra.io/docs/concepts/secret) and reference them with `{{ secret('K8S_TOKEN') }}`.

## Tasks

### PipelineRuns

Use `pipelinerun.Create` to create a run from `pipelineRef` or an inline `spec`, with `params`, `workspaces`, and `serviceAccountName`. Set `wait: true` to poll the Tekton `Succeeded` condition. A `False` condition fails the Kestra task; `waitTimeout` limits how long it waits and cancels the run if the timeout expires. `Get`, `List`, and `Cancel` read, filter, and cancel runs.

### TaskRuns

Use `taskrun.Create` and `taskrun.Get` for single Tekton Tasks. `Create` supports `taskRef`, inline `spec`, parameters, workspace bindings, optional waiting, `waitTimeout`, and cancellation when the Kestra execution is killed.

## Triggers

### PipelineRun trigger

`pipelinerun.Trigger` polls terminal PipelineRuns selected by `labelSelector`. Each poll is bounded by `limit` (100 by default); use a focused label selector for busy namespaces. It stores the last emitted run name and completion time in the namespace KV Store, so repeated polls do not emit the same run twice. The trigger exposes the run name, namespace, status, and completion time to the execution.

## Cluster requirements

The target cluster must expose the Tekton `tekton.dev/v1` API. The plugin does not install Tekton or create Kubernetes namespaces.
