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
@Schema(title = "Get a Tekton PipelineRun", description = "Reads a PipelineRun status and its results.")
@Plugin(
    examples = @Example(
        title = "Read a PipelineRun result.",
        full = true,
        code = """
            id: tekton_get
            namespace: company.team

            tasks:
              - id: get
                type: io.kestra.plugin.tekton.pipelinerun.Get
                namespace: ci
                name: build-and-test-run
            """
    )
)
public class Get extends AbstractTektonTask implements RunnableTask<TektonOutput> {
    @Schema(title = "PipelineRun name")
    @PluginProperty(group = "source")
    @NotNull
    private Property<String> name;

    @Override
    public TektonOutput run(RunContext runContext) throws Exception {
        String renderedNamespace = renderNamespace(runContext);
        String renderedName = runContext.render(name).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("name must render to a non-empty string"));
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            PipelineRun pipelineRun = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace).withName(renderedName).get();
            if (pipelineRun == null) {
                throw new IllegalStateException("PipelineRun '" + renderedName + "' was not found in namespace '" + renderedNamespace + "'");
            }
            var status = pipelineRun.getStatus();
            return TektonOutput.from(
                pipelineRun.getMetadata(),
                status == null ? List.of() : status.getConditions(),
                status == null ? null : status.getStartTime(),
                status == null ? null : status.getCompletionTime(),
                status == null ? List.of() : status.getResults()
            );
        }
    }
}
