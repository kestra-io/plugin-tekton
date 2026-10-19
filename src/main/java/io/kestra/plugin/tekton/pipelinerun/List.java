package io.kestra.plugin.tekton.pipelinerun;

import io.fabric8.tekton.v1.PipelineRun;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.tekton.AbstractTektonTask;
import io.kestra.plugin.tekton.TektonClientSupport;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Comparator;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(title = "List Tekton PipelineRuns", description = "Lists PipelineRuns, optionally filtering by labels and Succeeded status.")
@Plugin(
    examples = @Example(
        title = "List failed PipelineRuns.",
        full = true,
        code = """
            id: tekton_list
            namespace: company.team

            tasks:
              - id: failed
                type: io.kestra.plugin.tekton.pipelinerun.List
                namespace: ci
                labelSelector: tekton.dev/pipeline=build-and-test
                status: False
            """
    )
)
public class List extends AbstractTektonTask implements RunnableTask<List.Output> {
    @Schema(title = "Label selector", description = "Kubernetes label selector applied by the API server.")
    @PluginProperty(group = "advanced")
    private Property<String> labelSelector;

    @Schema(title = "Succeeded status", description = "Filter by the Tekton Succeeded condition: True, False, or Unknown.")
    @PluginProperty(group = "advanced")
    private Property<String> status;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedNamespace = renderNamespace(runContext);
        String selector = labelSelector == null ? null : runContext.render(labelSelector).as(String.class).orElse(null);
        String renderedStatus = status == null ? null : runContext.render(status).as(String.class).orElse(null);
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            var operation = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace);
            var runs = selector != null && !selector.isBlank()
                ? operation.withLabelSelector(selector).list().getItems()
                : operation.list().getItems();
            var items = runs.stream()
                .filter(run -> renderedStatus == null || renderedStatus.equals(status(run)))
                .sorted(Comparator.comparing(run -> run.getMetadata().getName()))
                .map(this::output)
                .toList();
            return Output.builder().items(items).size(items.size()).build();
        }
    }

    private String status(PipelineRun run) {
        return TektonClientSupport.conditionStatus(run.getStatus() == null ? null : run.getStatus().getConditions());
    }

    private TektonOutput output(PipelineRun run) {
        var current = run.getStatus();
        return TektonOutput.from(
            run.getMetadata(),
            current == null ? java.util.List.of() : current.getConditions(),
            current == null ? null : current.getStartTime(),
            current == null ? null : current.getCompletionTime(),
            current == null ? java.util.List.of() : current.getResults()
        );
    }

    @lombok.Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "PipelineRuns")
        private final java.util.List<TektonOutput> items;

        @Schema(title = "Number of matching PipelineRuns")
        private final Integer size;
    }
}
