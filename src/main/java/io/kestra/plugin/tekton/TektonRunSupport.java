package io.kestra.plugin.tekton;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.tekton.v1.Param;
import io.fabric8.tekton.v1.ParamValue;
import io.fabric8.tekton.v1.PipelineRunSpec;
import io.fabric8.tekton.v1.TaskRunSpec;
import io.fabric8.tekton.v1.WorkspaceBinding;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.tekton.model.TektonOutput;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TektonRunSupport {
    private TektonRunSupport() {
    }

    public static PipelineRunSpec pipelineSpec(RunContext runContext, String inlineSpec, String pipelineRef,
                                               Map<String, Object> params, List<Object> workspaces,
                                               String serviceAccountName) throws Exception {
        PipelineRunSpec spec = inlineSpec == null || inlineSpec.isBlank()
            ? new PipelineRunSpec()
            : JacksonMapper.ofYaml().readValue(inlineSpec, PipelineRunSpec.class);

        if (pipelineRef != null && !pipelineRef.isBlank()) {
            var ref = new io.fabric8.tekton.v1.PipelineRef();
            ref.setName(pipelineRef);
            spec.setPipelineRef(ref);
        }
        if (spec.getPipelineRef() == null && spec.getPipelineSpec() == null) {
            throw new IllegalArgumentException("Either pipelineRef or a pipelineSpec in spec must be provided");
        }
        spec.setParams(params(params));
        spec.setWorkspaces(workspaces(workspaces));
        if (serviceAccountName != null && !serviceAccountName.isBlank()) {
            var template = spec.getTaskRunTemplate();
            if (template == null) {
                template = new io.fabric8.tekton.v1.PipelineTaskRunTemplate();
            }
            template.setServiceAccountName(serviceAccountName);
            spec.setTaskRunTemplate(template);
        }
        return spec;
    }

    public static TaskRunSpec taskSpec(RunContext runContext, String inlineSpec, String taskRef,
                                       Map<String, Object> params, List<Object> workspaces,
                                       String serviceAccountName) throws Exception {
        TaskRunSpec spec = inlineSpec == null || inlineSpec.isBlank()
            ? new TaskRunSpec()
            : JacksonMapper.ofYaml().readValue(inlineSpec, TaskRunSpec.class);

        if (taskRef != null && !taskRef.isBlank()) {
            var ref = new io.fabric8.tekton.v1.TaskRef();
            ref.setName(taskRef);
            spec.setTaskRef(ref);
        }
        if (spec.getTaskRef() == null && spec.getTaskSpec() == null) {
            throw new IllegalArgumentException("Either taskRef or a taskSpec in spec must be provided");
        }
        spec.setParams(params(params));
        spec.setWorkspaces(workspaces(workspaces));
        if (serviceAccountName != null && !serviceAccountName.isBlank()) {
            spec.setServiceAccountName(serviceAccountName);
        }
        return spec;
    }

    private static List<Param> params(Map<String, Object> values) {
        if (values == null) {
            return new ArrayList<>();
        }
        return values.entrySet().stream().map(entry -> {
            Param param = new Param();
            param.setName(entry.getKey());
            param.setValue(new ParamValue(String.valueOf(entry.getValue())));
            return param;
        }).toList();
    }

    private static List<WorkspaceBinding> workspaces(List<Object> values) {
        if (values == null) {
            return new ArrayList<>();
        }
        return values.stream()
            .map(value -> JacksonMapper.ofJson().convertValue(value, WorkspaceBinding.class))
            .toList();
    }

    public static ObjectMeta metadata(String prefix) {
        ObjectMeta metadata = new ObjectMeta();
        metadata.setGenerateName(prefix + "-");
        return metadata;
    }

    public static Map<String, Object> renderMap(RunContext runContext, Map<String, Object> values) throws Exception {
        return values == null ? new LinkedHashMap<>() : runContext.render(values);
    }

    public static String renderString(RunContext runContext, io.kestra.core.models.property.Property<String> value) throws Exception {
        return value == null ? null : runContext.render(value).as(String.class).orElse(null);
    }

    public static TektonOutput requireSuccessful(TektonOutput output) {
        if ("False".equals(output.getStatus())) {
            throw new IllegalStateException("Tekton run '" + output.getName() + "' failed");
        }
        return output;
    }
}
