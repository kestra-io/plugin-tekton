package io.kestra.plugin.tekton.pipelinerun;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.tekton.v1.PipelineRun;
import io.fabric8.tekton.v1.PipelineRunSpec;
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
@Schema(
    title = "Create a Tekton PipelineRun",
    description = "Creates a PipelineRun in a Kubernetes cluster and optionally waits for its terminal status."
)
@Plugin(
    examples = @Example(
        title = "Run a Tekton pipeline and wait for it to finish.",
        full = true,
        code = """
            id: tekton_pipeline
            namespace: company.team

            tasks:
              - id: run
                type: io.kestra.plugin.tekton.pipelinerun.Create
                namespace: ci
                connection:
                  oauthToken: "{{ secret('K8S_TOKEN') }}"
                  masterUrl: "{{ secret('K8S_MASTER_URL') }}"
                pipelineRef: build-and-test
                params:
                  git-url: https://github.com/acme/app.git
                  revision: main
                serviceAccountName: tekton-runner
                wait: true
                timeout: PT30M
            """
    )
)
public class Create extends AbstractTektonTask implements RunnableTask<TektonOutput> {
    @Schema(title = "Pipeline name", description = "Name of an existing Tekton Pipeline.")
    @PluginProperty(group = "source")
    private Property<String> pipelineRef;

    @Schema(
        title = "Inline PipelineRun spec",
        description = "YAML for a Tekton PipelineRun spec. Use this instead of pipelineRef to provide pipelineSpec or advanced run settings."
    )
    @PluginProperty(group = "source")
    private Property<String> spec;

    @Schema(title = "Pipeline parameters", description = "Values passed to the referenced pipeline.")
    @PluginProperty(group = "source")
    private Property<Map<String, Object>> params;

    @Schema(title = "Workspace bindings", description = "Tekton workspace bindings in the same shape as a PipelineRun manifest.")
    @PluginProperty(group = "source")
    private Property<List<Object>> workspaces;

    @Schema(title = "Service account name", description = "Service account used by the TaskRuns created for this PipelineRun.")
    @PluginProperty(group = "advanced")
    private Property<String> serviceAccountName;

    @Schema(title = "Wait for completion", description = "When true, wait for the PipelineRun to reach a terminal Succeeded condition.")
    @PluginProperty(group = "execution")
    @NotNull
    @lombok.Builder.Default
    private final Property<Boolean> wait = Property.ofValue(false);

    @Schema(title = "Wait timeout", description = "Maximum time to wait for a terminal PipelineRun status.")
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
        String renderedPipelineRef = TektonRunSupport.renderString(runContext, pipelineRef);
        String renderedSpec = TektonRunSupport.renderString(runContext, spec);
        Map<String, Object> renderedParams = params == null ? Map.of() : runContext.render(params).asMap(String.class, Object.class);
        List<Object> renderedWorkspaces = workspaces == null ? List.of() : runContext.render(workspaces).asList(Object.class);
        String renderedServiceAccount = TektonRunSupport.renderString(runContext, serviceAccountName);

        PipelineRunSpec pipelineRunSpec = TektonRunSupport.pipelineSpec(
            runContext, renderedSpec, renderedPipelineRef, renderedParams, renderedWorkspaces, renderedServiceAccount
        );
        PipelineRun pipelineRun = new PipelineRun();
        pipelineRun.setMetadata(TektonRunSupport.metadata("kestra-pipeline"));
        pipelineRun.setSpec(pipelineRunSpec);

        killed.set(false);
        currentNamespace.set(renderedNamespace);
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            currentClient.set(client);
            PipelineRun created = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace).resource(pipelineRun).create();
            String name = created.getMetadata().getName();
            currentName.set(name);
            TektonOutput output = output(created);
            if (!runContext.render(wait).as(Boolean.class).orElse(false)) {
                return output;
            }
            output = waitForCompletion(runContext, client, renderedNamespace, name);
            return TektonRunSupport.requireSuccessful(output);
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
            PipelineRun current = client.tekton().v1().pipelineRuns().inNamespace(namespace).withName(name).get();
            if (current == null) {
                throw new IllegalStateException("PipelineRun '" + name + "' no longer exists");
            }
            TektonOutput output = output(current);
            if (TektonClientSupport.terminal(output.getStatus())) {
                return output;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out waiting for PipelineRun '" + name + "'");
            }
            Thread.sleep(runContext.render(pollInterval).as(Duration.class).orElseThrow().toMillis());
        }
    }

    private TektonOutput output(PipelineRun pipelineRun) {
        var status = pipelineRun.getStatus();
        return TektonOutput.from(
            pipelineRun.getMetadata(),
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
            PipelineRun pipelineRun = client.tekton().v1().pipelineRuns().inNamespace(namespace).withName(name).get();
            if (pipelineRun != null && pipelineRun.getSpec() != null) {
                pipelineRun.getSpec().setStatus("Cancelled");
                client.tekton().v1().pipelineRuns().inNamespace(namespace).resource(pipelineRun).replace();
            }
        } catch (Exception e) {
            // The execution is already being killed; a best-effort cancel must not mask that state.
        }
    }
}
