package io.kestra.plugin.tekton;

import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.WorkingDirFactory;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.kestra.plugin.tekton.pipelinerun.Cancel;
import io.kestra.plugin.tekton.pipelinerun.List;
import io.kestra.plugin.tekton.pipelinerun.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest(startApplication = false, startRunner = false, startScheduler = false, startWorker = false)
class TektonOperationsTest {
    private KubernetesMockServer server;
    private Connection connection;
    private KVStore store;
    private Map<String, Object> kvValues;
    private final TestRunContextFactory runContextFactory = new LocalTestRunContextFactory();

    @BeforeEach
    void setUp() throws Exception {
        server = new KubernetesMockServer(false);
        server.init();
        connection = Connection.builder()
            .masterUrl(Property.ofValue(server.url("/")))
            .build();
        store = mock(KVStore.class);
        kvValues = new HashMap<>();
        when(store.getValue(anyString())).thenAnswer(invocation -> Optional.ofNullable(kvValues.get(invocation.getArgument(0)))
            .map(KVValue::new));
        doAnswer(invocation -> {
            kvValues.put(invocation.getArgument(0), ((io.kestra.core.storages.kv.KVValueAndMetadata) invocation.getArgument(1)).value());
            return null;
        }).when(store).put(anyString(), any(), org.mockito.ArgumentMatchers.eq(true));
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

        Flow flow = Flow.builder().id("flow").namespace("team").build();
        Trigger trigger = Trigger.builder()
            .id("finished")
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        RunContext runContext = triggerRunContext(trigger);
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(flow)
            .runContext(runContext)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .flowId("flow")
            .triggerId("finished")
            .build();

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent(), "history must be checkpointed");

        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("run-c")));
        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent(), "an observed run must not repeat");
    }

    @Test
    void triggerWatermarkIsIsolatedPerFlowAndTrigger() throws Exception {
        String run = pipelineRun("history", "2024-01-01T00:01:00Z", "True");
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList(run)).times(2);

        Trigger trigger = Trigger.builder()
            .id("finished")
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        RunContext runContext = triggerRunContext(trigger);
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(Flow.builder().id("flow").namespace("team").build())
            .runContext(runContext)
            .build();

        assertFalse(trigger.evaluate(conditionContext, TriggerContext.builder()
            .flowId("flow-a").triggerId("finished").build()).isPresent());
        assertFalse(trigger.evaluate(conditionContext, TriggerContext.builder()
            .flowId("flow-b").triggerId("finished").build()).isPresent());
    }

    @Test
    void triggerDoesNotLoseRunsAfterAnInitiallyEmptyPoll() throws Exception {
        String path = "/apis/tekton.dev/v1/namespaces/ci/pipelineruns";
        String run = pipelineRun("new-run", "2024-01-01T00:01:00Z", "True");
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList()).once();
        server.expect().get().withPath(path + "?limit=100").andReturn(200, pipelineRunList(run)).once();

        Trigger trigger = Trigger.builder()
            .id("finished")
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        RunContext runContext = triggerRunContext(trigger);
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(Flow.builder().id("flow").namespace("team").build())
            .runContext(runContext)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .flowId("flow").triggerId("finished").build();

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent());
        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("new-run")));
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
            .andReturn(200, firstPage).once();
        server.expect().get().withPath(path + "?continue=page-2&limit=100")
            .andReturn(200, secondPage).once();
        server.expect().get().withPath(path + "?limit=100")
            .andReturn(200, pipelineRunListWithContinue(
                "page-2",
                pipelineRun("old-run", "2024-01-01T00:01:00Z", "True")
            )).once();
        server.expect().get().withPath(path + "?continue=page-2&limit=100")
            .andReturn(200, secondPage).once();

        Trigger trigger = Trigger.builder()
            .id("finished")
            .connection(connection)
            .namespace(Property.ofValue("ci"))
            .build();
        RunContext runContext = triggerRunContext(trigger);
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(Flow.builder().id("flow").namespace("team").build())
            .runContext(runContext)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .flowId("flow").triggerId("finished").build();

        assertFalse(trigger.evaluate(conditionContext, triggerContext).isPresent());
        Optional<io.kestra.core.models.executions.Execution> execution = trigger.evaluate(conditionContext, triggerContext);
        assertTrue(execution.isPresent());
        assertTrue(execution.orElseThrow().getLabels().stream()
            .anyMatch(label -> label.key().equals("tekton.pipelinerun.name") && label.value().equals("new-run")));
    }

    private RunContext runContext() {
        return runContextFactory.of("team", "flow");
    }

    private RunContext triggerRunContext(Trigger trigger) throws Exception {
        RunContext context = mock(RunContext.class);
        RunContextProperty<String> namespace = rendered("ci");
        RunContextProperty<Boolean> inheritClusterConfig = rendered(true);
        RunContextProperty<String> masterUrl = rendered(server.url("/"));
        RunContextProperty<?> defaults = renderedDefaults();
        doAnswer(invocation -> {
            Property<?> property = invocation.getArgument(0);
            if (property == trigger.getNamespace()) {
                return namespace;
            }
            if (property == trigger.getInheritClusterConfig()) {
                return inheritClusterConfig;
            }
            if (property == connection.getMasterUrl()) {
                return masterUrl;
            }
            return defaults;
        }).when(context).render(any(Property.class));
        when(context.namespaceKv(anyString())).thenReturn(store);
        when(context.flowInfo()).thenReturn(new RunContext.FlowInfo("tenant", "team", "flow", 1));
        return context;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> RunContextProperty<T> rendered(T value) throws Exception {
        RunContextProperty property = mock(RunContextProperty.class);
        doReturn(Optional.ofNullable(value)).when(property).as(any(Class.class));
        return property;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RunContextProperty<?> renderedDefaults() throws Exception {
        RunContextProperty property = mock(RunContextProperty.class);
        doAnswer(invocation -> {
            Class<?> type = invocation.getArgument(0);
            if (type == String.class) {
                return Optional.of(server.url("/"));
            }
            if (type == Boolean.class) {
                return Optional.of(true);
            }
            return Optional.empty();
        }).when(property).as(any(Class.class));
        return property;
    }

    private static final class LocalTestRunContextFactory extends TestRunContextFactory {
        private LocalTestRunContextFactory() {
            this.workingDirFactory = new LocalWorkingDirFactory();
        }
    }

    private static final class LocalWorkingDirFactory extends WorkingDirFactory {
        private LocalWorkingDirFactory() {
            this.tmpdirPath = Optional.empty();
        }
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
