package io.kestra.plugin.tekton.taskrun;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.tekton.v1.TaskRun;
import io.fabric8.tekton.v1.TaskRunSpec;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.tekton.AbstractTektonTask;
import io.kestra.plugin.tekton.TektonClientSupport;
import io.kestra.plugin.tekton.TektonRunSupport;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
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
public class Create extends AbstractTektonTask implements RunnableTask<TektonOutput> {
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

    @Schema(title = "Wait for completion")
    @PluginProperty(group = "execution")
    @NotNull
    @lombok.Builder.Default
    private final Property<Boolean> wait = Property.ofValue(false);

    @Schema(title = "Wait timeout")
    @PluginProperty(group = "execution")
    @NotNull
    @lombok.Builder.Default
    private final Property<Duration> timeout = Property.ofValue(Duration.ofMinutes(30));

    @JsonIgnore
    private final AtomicBoolean killed = new AtomicBoolean();
    @JsonIgnore
    private final AtomicReference<String> currentName = new AtomicReference<>();
    @JsonIgnore
    private final AtomicReference<String> currentNamespace = new AtomicReference<>();
    @JsonIgnore
    private final AtomicReference<TektonClientSupport.ClientHandle> currentClient = new AtomicReference<>();

    @Override
    public TektonOutput run(RunContext runContext) throws Exception {
        String renderedNamespace = renderNamespace(runContext);
        String renderedTaskRef = TektonRunSupport.renderString(runContext, taskRef);
        String renderedSpec = TektonRunSupport.renderString(runContext, spec);
        Map<String, Object> renderedParams = params == null ? Map.of() : runContext.render(params).asMap(String.class, Object.class);
        List<Object> renderedWorkspaces = workspaces == null ? List.of() : runContext.render(workspaces).asList(Object.class);
        String renderedServiceAccount = TektonRunSupport.renderString(runContext, serviceAccountName);
        TaskRunSpec taskRunSpec = TektonRunSupport.taskSpec(
            runContext, renderedSpec, renderedTaskRef, renderedParams, renderedWorkspaces, renderedServiceAccount
        );
        TaskRun taskRun = new TaskRun();
        taskRun.setMetadata(TektonRunSupport.metadata("kestra-task"));
        taskRun.setSpec(taskRunSpec);

        killed.set(false);
        currentNamespace.set(renderedNamespace);
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            currentClient.set(client);
            TaskRun created = client.tekton().v1().taskRuns().inNamespace(renderedNamespace).resource(taskRun).create();
            String name = created.getMetadata().getName();
            currentName.set(name);
            if (!runContext.render(wait).as(Boolean.class).orElse(false)) {
                return output(created);
            }
            return TektonRunSupport.requireSuccessful(waitForCompletion(runContext, client, renderedNamespace, name));
        } finally {
            currentClient.set(null);
            currentName.set(null);
            currentNamespace.set(null);
        }
    }

    private TektonOutput waitForCompletion(RunContext runContext, TektonClientSupport.ClientHandle client,
                                           String namespace, String name) throws Exception {
        Duration waitTimeout = runContext.render(timeout).as(Duration.class).orElseThrow();
        long deadline = System.nanoTime() + waitTimeout.toNanos();
        while (true) {
            TaskRun current = client.tekton().v1().taskRuns().inNamespace(namespace).withName(name).get();
            if (current == null) {
                throw new IllegalStateException("TaskRun '" + name + "' no longer exists");
            }
            TektonOutput output = output(current);
            if (TektonClientSupport.terminal(output.getStatus())) {
                return output;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out waiting for TaskRun '" + name + "'");
            }
            Thread.sleep(runContext.render(pollInterval).as(Duration.class).orElseThrow().toMillis());
        }
    }

    private TektonOutput output(TaskRun taskRun) {
        var status = taskRun.getStatus();
        return TektonOutput.from(
            taskRun.getMetadata(),
            status == null ? List.of() : status.getConditions(),
            status == null ? null : status.getStartTime(),
            status == null ? null : status.getCompletionTime(),
            status == null ? List.of() : status.getResults()
        );
    }

    @Override
    public void kill() {
        if (!killed.compareAndSet(false, true)) {
            return;
        }
        TektonClientSupport.ClientHandle client = currentClient.get();
        String namespace = currentNamespace.get();
        String name = currentName.get();
        if (client == null || namespace == null || name == null) {
            return;
        }
        try {
            TaskRun taskRun = client.tekton().v1().taskRuns().inNamespace(namespace).withName(name).get();
            if (taskRun != null && taskRun.getSpec() != null) {
                taskRun.getSpec().setStatus("Cancelled");
                client.tekton().v1().taskRuns().inNamespace(namespace).resource(taskRun).replace();
            }
        } catch (Exception ignored) {
            // Best effort: the Kestra execution is already being killed.
        }
    }
}
