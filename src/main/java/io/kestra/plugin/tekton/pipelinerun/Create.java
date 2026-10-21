package io.kestra.plugin.tekton.pipelinerun;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.tekton.client.TektonClient;
import io.fabric8.tekton.v1.PipelineRun;
import io.fabric8.tekton.v1.PipelineRunSpec;
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
                waitTimeout: PT30M
            """
    )
)
public class Create extends AbstractTektonCreate<PipelineRun> {
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

        return execute(runContext, renderedNamespace, pipelineRun);
    }

    @Override
    protected PipelineRun create(TektonClient client, String namespace, PipelineRun resource) {
        return client.v1().pipelineRuns().inNamespace(namespace).resource(resource).create();
    }

    @Override
    protected Resource<PipelineRun> resource(TektonClient client, String namespace, String name) {
        return client.v1().pipelineRuns().inNamespace(namespace).withName(name);
    }

    @Override
    protected Resource<PipelineRun> resource(TektonClient client, String namespace, PipelineRun resource) {
        return client.v1().pipelineRuns().inNamespace(namespace).resource(resource);
    }

    @Override
    protected void markCancelled(PipelineRun pipelineRun) {
        if (pipelineRun.getSpec() != null) {
            pipelineRun.getSpec().setStatus("Cancelled");
        }
    }

    @Override
    protected String resourceType() {
        return "PipelineRun";
    }

    @Override
    protected TektonOutput output(PipelineRun pipelineRun) {
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
