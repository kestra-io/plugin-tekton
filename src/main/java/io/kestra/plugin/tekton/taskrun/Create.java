package io.kestra.plugin.tekton.taskrun;

import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.tekton.client.TektonClient;
import io.fabric8.tekton.v1.TaskRun;
import io.fabric8.tekton.v1.TaskRunSpec;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.tekton.AbstractTektonCreate;
import io.kestra.plugin.tekton.TektonClientSupport;
import io.kestra.plugin.tekton.TektonRunSupport;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(title = "Create a Tekton TaskRun", description = "Creates a TaskRun and optionally waits for its terminal status.")
@Plugin(
    examples = @Example(
        title = "Run a Tekton Task.",
        full = true,
        code = """
            id: tekton_task
            namespace: company.team

            tasks:
              - id: run
                type: io.kestra.plugin.tekton.taskrun.Create
                namespace: ci
                taskRef: unit-tests
                wait: true
            """
    )
)
public class Create extends AbstractTektonCreate<TaskRun> {
    @Schema(title = "Task name", description = "Name of an existing Tekton Task.")
    @PluginProperty(group = "source")
    private Property<String> taskRef;

    @Schema(title = "Inline TaskRun spec", description = "YAML for a Tekton TaskRun spec. Use this instead of taskRef for an inline taskSpec.")
    @PluginProperty(group = "source")
    private Property<String> spec;

    @Schema(title = "Task parameters")
    @PluginProperty(group = "source")
    private Property<Map<String, Object>> params;

    @Schema(title = "Workspace bindings")
    @PluginProperty(group = "source")
    private Property<List<Object>> workspaces;

    @Schema(title = "Service account name")
    @PluginProperty(group = "advanced")
    private Property<String> serviceAccountName;

    @Override
    public TektonOutput run(RunContext runContext) throws Exception {
        String rNamespace = renderNamespace(runContext);
        String rTaskRef = TektonRunSupport.renderString(runContext, taskRef);
        String rSpec = TektonRunSupport.renderString(runContext, spec);
        Map<String, Object> rParams = params == null ? Map.of() : runContext.render(params).asMap(String.class, Object.class);
        List<Object> rWorkspaces = workspaces == null ? List.of() : runContext.render(workspaces).asList(Object.class);
        String rServiceAccount = TektonRunSupport.renderString(runContext, serviceAccountName);
        TaskRunSpec taskRunSpec = TektonRunSupport.taskSpec(
            runContext, rSpec, rTaskRef, rParams, rWorkspaces, rServiceAccount
        );
        TaskRun taskRun = new TaskRun();
        taskRun.setMetadata(TektonRunSupport.metadata("kestra-task"));
        taskRun.setSpec(taskRunSpec);

        return execute(runContext, rNamespace, taskRun);
    }

    @Override
    protected TaskRun create(TektonClient client, String namespace, TaskRun resource) {
        return client.v1().taskRuns().inNamespace(namespace).resource(resource).create();
    }

    @Override
    protected Resource<TaskRun> resource(TektonClient client, String namespace, String name) {
        return client.v1().taskRuns().inNamespace(namespace).withName(name);
    }

    @Override
    protected Resource<TaskRun> resource(TektonClient client, String namespace, TaskRun resource) {
        return client.v1().taskRuns().inNamespace(namespace).resource(resource);
    }

    @Override
    protected void markCancelled(TaskRun taskRun) {
        if (taskRun.getSpec() != null) {
            taskRun.getSpec().setStatus("TaskRunCancelled");
        }
    }

    @Override
    protected String resourceType() {
        return "TaskRun";
    }

    @Override
    protected TektonOutput output(TaskRun taskRun) {
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
