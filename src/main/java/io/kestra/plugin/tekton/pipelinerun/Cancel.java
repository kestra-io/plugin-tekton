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
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(title = "Cancel a Tekton PipelineRun", description = "Requests cancellation by setting spec.status to Cancelled.")
@Plugin(
    examples = @Example(
        title = "Cancel a PipelineRun.",
        full = true,
        code = """
            id: tekton_cancel
            namespace: company.team

            tasks:
              - id: cancel
                type: io.kestra.plugin.tekton.pipelinerun.Cancel
                namespace: ci
                name: build-and-test-run
            """
    )
)
public class Cancel extends AbstractTektonTask implements RunnableTask<TektonOutput> {
    @Schema(title = "PipelineRun name")
    @PluginProperty(group = "source")
    @NotNull
    private Property<String> name;

    @Override
    public TektonOutput run(RunContext runContext) throws Exception {
        String renderedNamespace = renderNamespace(runContext);
        String renderedName = runContext.render(name).as(String.class).orElseThrow();
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            PipelineRun pipelineRun = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace).withName(renderedName).get();
            if (pipelineRun == null) {
                throw new IllegalStateException("PipelineRun '" + renderedName + "' was not found");
            }
            if (pipelineRun.getSpec() == null) {
                pipelineRun.setSpec(new io.fabric8.tekton.v1.PipelineRunSpec());
            }
            PipelineRun cancelled = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace).withName(renderedName).edit(current -> {
                if (current.getSpec() == null) {
                    current.setSpec(new io.fabric8.tekton.v1.PipelineRunSpec());
                }
                current.getSpec().setStatus("Cancelled");
                return current;
            });
            var status = cancelled.getStatus();
            return TektonOutput.from(
                cancelled.getMetadata(),
                status == null ? List.of() : status.getConditions(),
                status == null ? null : status.getStartTime(),
                status == null ? null : status.getCompletionTime(),
                status == null ? List.of() : status.getResults()
            );
        }
    }
}
