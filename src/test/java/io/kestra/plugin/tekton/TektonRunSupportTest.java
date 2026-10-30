package io.kestra.plugin.tekton;

import io.fabric8.knative.pkg.apis.Condition;
import io.fabric8.tekton.v1.ParamValue;
import io.fabric8.tekton.v1.TaskRunSpec;
import io.fabric8.tekton.v1.PipelineRunSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TektonRunSupportTest {
    @Test
    void buildsPipelineSpecFromReferenceAndRenderedValues() throws Exception {
        PipelineRunSpec spec = TektonRunSupport.pipelineSpec(
            null,
            null,
            "build-and-test",
            Map.of("revision", "main"),
            List.of(Map.of("name", "source", "emptyDir", Map.of())),
            "tekton-runner"
        );

        assertEquals("build-and-test", spec.getPipelineRef().getName());
        assertEquals("revision", spec.getParams().getFirst().getName());
        assertEquals("main", spec.getParams().getFirst().getValue().getStringVal());
        assertEquals("source", spec.getWorkspaces().getFirst().getName());
        assertEquals("tekton-runner", spec.getTaskRunTemplate().getServiceAccountName());
    }

    @Test
    void rejectsRunWithoutReferenceOrInlineSpec() {
        assertThrows(IllegalArgumentException.class, () -> TektonRunSupport.pipelineSpec(null, null, null, null, null, null));
    }

    @Test
    void preservesTypedTaskParameters() throws Exception {
        TaskRunSpec spec = TektonRunSupport.taskSpec(
            null,
            null,
            "unit-tests",
            Map.of(
                "scalar", "value",
                "array", List.of("one", "two"),
                "object", Map.of("key", "value")
            ),
            null,
            null
        );

        var params = spec.getParams().stream().collect(java.util.stream.Collectors.toMap(io.fabric8.tekton.v1.Param::getName, io.fabric8.tekton.v1.Param::getValue));
        assertEquals("string", params.get("scalar").getType());
        assertEquals("value", params.get("scalar").getStringVal());
        assertEquals("array", params.get("array").getType());
        assertEquals(List.of("one", "two"), params.get("array").getArrayVal());
        assertEquals("object", params.get("object").getType());
        assertEquals(Map.of("key", "value"), params.get("object").getObjectVal());
    }

    @Test
    void readsSucceededConditionStatus() {
        Condition condition = new Condition();
        condition.setType("Succeeded");
        condition.setStatus("False");

        assertEquals("False", TektonClientSupport.conditionStatus(List.of(condition)));
        assertEquals("Unknown", TektonClientSupport.conditionStatus(List.of()));
        assertEquals(true, TektonClientSupport.terminal("True"));
        assertEquals(false, TektonClientSupport.terminal("Unknown"));
    }

    @Test
    void leavesStringResultsUnquoted() {
        assertEquals(
            Map.of("message", "hello", "items", "[\"one\",\"two\"]", "metadata", "{\"key\":\"value\"}"),
            TektonClientSupport.results(List.of(
                Map.of("name", "message", "value", "hello"),
                Map.of("name", "items", "value", List.of("one", "two")),
                Map.of("name", "metadata", "value", Map.of("key", "value"))
            ))
        );
    }
}
