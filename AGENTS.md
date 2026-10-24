# Kestra Tekton Plugin

## What

- Provides plugin components under `io.kestra.plugin.tekton`.
- Includes PipelineRun and TaskRun tasks and a PipelineRun polling trigger.

## Why

- What user problem does this solve? Teams need a concrete starting point for building and validating new Kestra plugins without recreating the same project scaffolding from scratch.
- Why would a team adopt this plugin in a workflow? It gives plugin authors a ready-made reference repo they can adapt alongside their own build, test, and publishing workflow.
- What operational/business outcome does it enable? It shortens plugin delivery time, reduces setup mistakes, and makes internal or partner plugin development more repeatable.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `tekton`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.tekton.pipelinerun.Create`
- `io.kestra.plugin.tekton.pipelinerun.Get`
- `io.kestra.plugin.tekton.pipelinerun.List`
- `io.kestra.plugin.tekton.pipelinerun.Cancel`
- `io.kestra.plugin.tekton.pipelinerun.Trigger`
- `io.kestra.plugin.tekton.taskrun.Create`
- `io.kestra.plugin.tekton.taskrun.Get`

### Project Structure

```
plugin-tekton/
├── src/main/java/io/kestra/plugin/tekton/
├── src/test/java/io/kestra/plugin/tekton/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
