package io.kestra.plugin.tekton;

import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.kestra.plugin.tekton.pipelinerun.Cancel;
import io.kestra.plugin.tekton.pipelinerun.Get;
import io.kestra.plugin.tekton.pipelinerun.List;
import io.kestra.plugin.tekton.pipelinerun.Trigger;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest(startApplication = false, startRunner = false, startScheduler = false, startWorker = false)
class TektonOperationsTest {
    private KubernetesMockServer server;
    private Connection connection;
    @Inject
    private TestRunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws Exception {
        server = new KubernetesMockServer(false);
        server.init();
        connection = Connection.builder()
            .masterUrl(Property.ofValue(server.url("/")))
            .build();
    }

    @AfterEach
    void tearDown() {
        server.destroy();
    }

    @Test
    void createsListsAndCancelsPipelineRunsThroughMockServer() throws Exception {
        String created = pipelineRun("created-pipeline", "2024-01-01T00:01:00Z", "True");
        server.expect().post().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns")
            .andReturn(201, created).once();

        RunContext runContext = runContext();
        TektonOutput createdOutput = io.kestra.plugin.tekton.pipelinerun.Create.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .pipelineRef(Property.ofValue("build-and-test"))
            .params(Property.ofValue(Map.of("revision", "main")))
            .build()
            .run(runContext);

        assertEquals("created-pipeline", createdOutput.getName());
        assertEquals("True", createdOutput.getStatus());
        assertTrue(server.takeRequest().getUtf8Body().contains("build-and-test"));

        server.expect().get().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns?limit=100")
            .andReturn(200, pipelineRunList(created)).once();
        List.Output listed = List.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build()
            .run(runContext);

        assertEquals(1, listed.getSize());
        assertEquals("created-pipeline", listed.getItems().getFirst().getName());
        server.takeRequest();

        String cancelled = pipelineRun("created-pipeline", "2024-01-01T00:02:00Z", "False");
        server.expect().get().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns/created-pipeline")
            .andReturn(200, created).once();
        server.expect().get().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns/created-pipeline")
            .andReturn(200, created).once();
        server.expect().patch().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns/created-pipeline")
            .andReturn(200, cancelled).once();

        TektonOutput cancelledOutput = Cancel.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .name(Property.ofValue("created-pipeline"))
            .build()
            .run(runContext);

        assertEquals("False", cancelledOutput.getStatus());
        server.takeRequest();
        server.takeRequest();
        assertTrue(server.takeRequest().getUtf8Body().contains("Cancelled"));
    }

    @Test
    void listPaginatesBeforeApplyingStatusFilter() throws Exception {
        String firstPage = pipelineRunListWithContinue(
            "page-2",
            pipelineRun("running", "2024-01-01T00:01:00Z", "Unknown")
        );
        String secondPage = pipelineRunList(
            pipelineRun("failed", "2024-01-01T00:02:00Z", "False")
        );
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        server.expect().get().withPath(path + "?limit=1")
            .andReturn(200, firstPage).once();
        server.expect().get().withPath(path + "?continue=page-2&limit=1")
            .andReturn(200, secondPage).once();

        List.Output output = List.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .status(Property.ofValue(List.SucceededStatus.False))
            .limit(Property.ofValue(1))
            .build()
            .run(runContext());

        assertEquals(1, output.getSize());
        assertEquals("failed", output.getItems().getFirst().getName());
    }

    @Test
    void rejectsListLimitOutsideSupportedRange() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> List.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .limit(Property.ofValue(0))
            .build()
            .run(runContext()));

        assertEquals("limit must be between 1 and 1000", exception.getMessage());
    }

    @Test
    void createsAndReadsTaskRunsThroughMockServer() throws Exception {
        String created = taskRun("created-task", "2024-01-01T00:01:00Z", "True");
        server.expect().post().withPath("/apis/tekton.dev/v1/namespaces/ci/taskruns")
            .andReturn(201, created).once();

        RunContext runContext = runContext();
        TektonOutput createdOutput = io.kestra.plugin.tekton.taskrun.Create.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .taskRef(Property.ofValue("unit-tests"))
            .build()
            .run(runContext);

        assertEquals("created-task", createdOutput.getName());
        assertEquals("True", createdOutput.getStatus());
        assertTrue(server.takeRequest().getUtf8Body().contains("unit-tests"));

        server.expect().get().withPath("/apis/tekton.dev/v1/namespaces/ci/taskruns/created-task")
            .andReturn(200, created).once();
        TektonOutput readOutput = io.kestra.plugin.tekton.taskrun.Get.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .name(Property.ofValue("created-task"))
            .build()
            .run(runContext);

        assertEquals("created-task", readOutput.getName());
        assertEquals("True", readOutput.getStatus());
    }

    @Test
    void readsPipelineRunsThroughMockServer() throws Exception {
        String run = pipelineRun("read-pipeline", "2024-01-01T00:01:00Z", "True");
        server.expect().get().withPath("/apis/tekton.dev/v1/namespaces/ci/pipelineruns/read-pipeline")
            .andReturn(200, run).once();

        TektonOutput output = Get.builder()
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .name(Property.ofValue("read-pipeline"))
            .build()
            .run(runContext());

        assertEquals("read-pipeline", output.getName());
        assertEquals("True", output.getStatus());
    }

    @Test
    void triggerInitializesWatermarkAndEmitsRunsInTimestampAndNameOrder() throws Exception {
        String first = pipelineRun("run-a", "2024-01-01T00:01:00Z", "True");
        String second = pipelineRun("run-b", "2024-01-01T00:01:00Z", "True");
        String third = pipelineRun("run-c", "2024-01-01T00:01:00Z", "True");
        String firstPoll = pipelineRunList(first, second);
        String secondPoll = pipelineRunList(third, first, second);
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        server.expect().get().withPath(path + "?limit=100").andReturn(200, firstPoll).once();
        server.expect().get().withPath(path + "?limit=100").andReturn(200, secondPoll).once();
        server.expect().get().withPath(path + "?limit=100").andReturn(200, secondPoll).once();

        String flowId = "ordering-" + UUID.randomUUID();
        Flow flow = Flow.builder().id(flowId).namespace("team").tenantId("main").revision(1).build();
        Trigger trigger = Trigger.builder()
            .id("finished")
            .type(Trigger.class.getName())
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        TriggerContext triggerContext = TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(flowId)
            .triggerId("finished")
            .build();
        var conditionContext = conditionContext(flow, trigger, triggerContext);

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent(), "history must be checkpointed");

        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertEquals("run-c", execution.orElseThrow().getTrigger().getVariables().get("name"));
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("run-c")));
        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent(), "an observed run must not repeat");
    }

    @Test
    void triggerWatermarkIsIsolatedForAmbiguousFlowAndTriggerIds() throws Exception {
        String history = pipelineRun("history", "2024-01-01T00:01:00Z", "True");
        String next = pipelineRun("next", "2024-01-01T00:02:00Z", "True");
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList(history)).times(2);
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList(next, history)).times(2);

        Trigger trigger = Trigger.builder()
            .id("finished")
            .type(Trigger.class.getName())
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        String prefix = UUID.randomUUID().toString();
        String firstFlowId = prefix + "-a-b";
        String secondFlowId = prefix + "-a";
        var firstFlow = Flow.builder().id(firstFlowId).namespace("team").tenantId("main").revision(1).build();
        var conditionContext = conditionContext(
            firstFlow, trigger, TriggerContext.builder().namespace("team").tenantId("main").flowId(firstFlowId).triggerId("c").build()
        );

        assertFalse(trigger.evaluate(conditionContext, TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(firstFlowId).triggerId("c").build()).isPresent());
        assertFalse(trigger.evaluate(conditionContext, TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(secondFlowId).triggerId("b-c").build()).isPresent());
        assertTrue(trigger.evaluate(conditionContext, TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(firstFlowId).triggerId("c").build()).isPresent());
        assertTrue(trigger.evaluate(conditionContext, TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(secondFlowId).triggerId("b-c").build()).isPresent());
    }

    @Test
    void triggerDoesNotLoseRunsAfterAnInitiallyEmptyPoll() throws Exception {
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        String run = pipelineRun("new-run", "2024-01-01T00:01:00Z", "True");
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList()).once();
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList(run)).once();

        Trigger trigger = Trigger.builder()
            .id("finished")
            .type(Trigger.class.getName())
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        String flowId = "empty-" + UUID.randomUUID();
        var flow = Flow.builder().id(flowId).namespace("team").tenantId("main").revision(1).build();
        TriggerContext triggerContext = TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(flowId).triggerId("finished").build();
        var conditionContext = conditionContext(flow, trigger, triggerContext);

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent());
        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("new-run")));
    }

    @Test
    void rejectsTriggerLimitOutsideSupportedRange() {
        String flowId = "invalid-limit-" + UUID.randomUUID();
        Trigger trigger = Trigger.builder()
            .id("finished")
            .type(Trigger.class.getName())
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .limit(Property.ofValue(1001))
            .build();
        RunContext runContext = runContextFactory.of("team", flowId);
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(Flow.builder().id(flowId).namespace("team").build())
            .runContext(runContext)
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(
            conditionContext,
            TriggerContext.builder().namespace("team").tenantId("main").flowId(flowId).triggerId("finished").build()
        ));

        assertEquals("limit must be between 1 and 1000", exception.getMessage());
    }

    @Test
    void triggerPaginatesAcrossPipelineRunPages() throws Exception {
        String firstPage = pipelineRunListWithContinue(
            "page-2",
            pipelineRun("old-run", "2024-01-01T00:01:00Z", "True")
        );
        String secondPage = pipelineRunList(
            pipelineRun("new-run", "2024-01-01T00:02:00Z", "True")
        );
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        server.expect().get().withPath(path + "?limit=100")
            .andReturn(200, pipelineRunList(pipelineRun("old-run", "2024-01-01T00:01:00Z", "True"))).once();
        server.expect().get().withPath(path + "?limit=100")
            .andReturn(200, firstPage).once();
        server.expect().get().withPath(path + "?continue=page-2&limit=100")
            .andReturn(200, secondPage).once();

        Trigger trigger = Trigger.builder()
            .id("finished")
            .type(Trigger.class.getName())
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        String flowId = "pagination-" + UUID.randomUUID();
        var flow = Flow.builder().id(flowId).namespace("team").tenantId("main").revision(1).build();
        TriggerContext triggerContext = TriggerContext.builder().namespace("team").tenantId("main")
            .flowId(flowId).triggerId("finished").build();
        var conditionContext = conditionContext(flow, trigger, triggerContext);

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent());
        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertEquals("new-run", execution.orElseThrow().getTrigger().getVariables().get("name"));
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("new-run")));
    }

    private ConditionContext conditionContext(Flow flow, Trigger trigger, TriggerContext triggerContext) {
        var runContext = runContextFactory.initializer()
            .forScheduler((DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger);
        return ConditionContext.builder().flow(flow).runContext(runContext).build();
    }

    private RunContext runContext() {
        return runContextFactory.of("team", "flow");
    }

    private static String pipelineRun(String name, String completionTime, String status) {
        return """
            {
              "apiVersion":"tekton.dev/v1",
              "kind":"PipelineRun",
              "metadata":{"name":"%s","namespace":"ci","creationTimestamp":"2024-01-01T00:00:00Z"},
              "spec":{"pipelineRef":{"name":"build-and-test"}},
              "status":{"conditions":[{"type":"Succeeded","status":"%s"}],"completionTime":"%s","startTime":"2024-01-01T00:00:00Z","results":[]}
            }
            """.formatted(name, status, completionTime);
    }

    private static String taskRun(String name, String completionTime, String status) {
        return """
            {
              "apiVersion":"tekton.dev/v1",
              "kind":"TaskRun",
              "metadata":{"name":"%s","namespace":"ci","creationTimestamp":"2024-01-01T00:00:00Z"},
              "spec":{"taskRef":{"name":"unit-tests"}},
              "status":{"conditions":[{"type":"Succeeded","status":"%s"}],"completionTime":"%s","startTime":"2024-01-01T00:00:00Z","results":[]}
            }
            """.formatted(name, status, completionTime);
    }

    private static String pipelineRunList(String... runs) {
        return """
            {"apiVersion":"tekton.dev/v1","kind":"PipelineRunList","items":[%s]}
            """.formatted(String.join(",", runs));
    }

    private static String pipelineRunListWithContinue(String continuation, String... runs) {
        return """
            {"apiVersion":"tekton.dev/v1","kind":"PipelineRunList","metadata":{"continue":"%s"},"items":[%s]}
            """.formatted(continuation, String.join(",", runs));
    }
}
