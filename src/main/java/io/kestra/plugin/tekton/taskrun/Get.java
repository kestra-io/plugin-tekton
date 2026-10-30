package io.kestra.plugin.tekton.taskrun;

import io.fabric8.tekton.v1.TaskRun;
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
@Schema(title = "Get a Tekton TaskRun", description = "Reads a TaskRun status and its results.")
@Plugin(
    examples = @Example(
        title = "Read a TaskRun result.",
        full = true,
        code = """
            id: tekton_task_get
            namespace: company.team

            tasks:
              - id: get
                type: io.kestra.plugin.tekton.taskrun.Get
                namespace: ci
                name: unit-tests-run
            """
    )
)
public class Get extends AbstractTektonTask implements RunnableTask<TektonOutput> {
    @Schema(title = "TaskRun name")
    @PluginProperty(group = "source")
    @NotNull
    private Property<String> name;

    @Override
    public TektonOutput run(RunContext runContext) throws Exception {
        String rNamespace = renderNamespace(runContext);
        String rName = runContext.render(name).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("name must render to a non-empty string"));
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            TaskRun taskRun = client.tekton().v1().taskRuns().inNamespace(rNamespace).withName(rName).get();
            if (taskRun == null) {
                throw new IllegalStateException("TaskRun '" + rName + "' was not found in namespace '" + rNamespace + "'");
            }
            var status = taskRun.getStatus();
            return TektonOutput.from(
                taskRun.getMetadata(),
                status == null ? List.of() : status.getConditions(),
                status == null ? null : status.getStartTime(),
                status == null ? null : status.getCompletionTime(),
                status == null ? List.of() : status.getResults()
            );
        }
    }
}
