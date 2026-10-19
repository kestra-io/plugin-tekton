package io.kestra.plugin.tekton.pipelinerun;

import io.fabric8.tekton.v1.PipelineRun;
import io.kestra.core.models.Label;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.kestra.plugin.tekton.TektonClientSupport;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on completed Tekton PipelineRuns",
    description = "Polls terminal PipelineRuns and stores a namespace KV Store watermark to avoid duplicate executions."
)
@Plugin(
    examples = @Example(
        title = "React when a PipelineRun finishes.",
        full = true,
        code = """
            id: tekton_finished
            namespace: company.team

            triggers:
              - id: run_finished
                type: io.kestra.plugin.tekton.pipelinerun.Trigger
                namespace: ci
                labelSelector: tekton.dev/pipeline=build-and-test
                interval: PT1M

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "PipelineRun {{ trigger.name }} ended with {{ trigger.status }}"
            """
    )
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface {
    private static final String WATERMARK_KEY = "tekton-pipelinerun-watermark";

    @Schema(title = "Polling interval")
    @PluginProperty(group = "execution")
    @lombok.Builder.Default
    private final Duration interval = Duration.ofMinutes(1);

    @Schema(title = "Kubernetes connection")
    @PluginProperty(group = "connection")
    private Connection connection;

    @Schema(title = "Tekton namespace")
    @PluginProperty(group = "connection")
    @lombok.Builder.Default
    private final Property<String> namespace = Property.ofValue("default");

    @Schema(title = "Label selector", description = "Kubernetes label selector applied to PipelineRuns.")
    @PluginProperty(group = "advanced")
    private Property<String> labelSelector;

    @Schema(title = "Inherit cluster auto-configuration")
    @PluginProperty(group = "advanced")
    @lombok.Builder.Default
    private final Property<Boolean> inheritClusterConfig = Property.ofValue(true);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        String renderedNamespace = runContext.render(namespace).as(String.class).orElseThrow();
        String selector = labelSelector == null ? null : runContext.render(labelSelector).as(String.class).orElse(null);
        List<PipelineRun> runs;
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(
            connection, runContext, runContext.render(inheritClusterConfig).as(Boolean.class).orElse(true)
        )) {
            var operation = client.tekton().v1().pipelineRuns().inNamespace(renderedNamespace);
            runs = selector == null || selector.isBlank()
                ? operation.list().getItems()
                : operation.withLabelSelector(selector).list().getItems();
        }

        List<PipelineRun> terminalRuns = runs.stream()
            .filter(this::terminal)
            .sorted(Comparator.comparing(this::watermark).thenComparing(run -> run.getMetadata().getName()))
            .toList();

        String watermarkKey = watermarkKey(triggerContext);
        KVStore store = runContext.namespaceKv(runContext.flowInfo().namespace());
        Watermark watermark = read(store, watermarkKey);
        PipelineRun next = terminalRuns.stream()
            .filter(run -> newerThan(run, watermark))
            .findFirst()
            .orElse(null);
        if (next == null) {
            return Optional.empty();
        }

        Watermark nextWatermark = new Watermark(next.getMetadata().getName(), watermark(next));
        store.put(
            watermarkKey,
            new KVValueAndMetadata(new KVMetadata("Last Tekton PipelineRun observed by this trigger", Duration.ofDays(3650)), nextWatermark),
            true
        );
        String status = status(next);
        Map<String, Object> triggerData = Map.of(
            "name", next.getMetadata().getName(),
            "namespace", renderedNamespace,
            "status", status,
            "completionTime", watermark(next)
        );
        return Optional.of(Execution.newExecution(
            conditionContext.getFlow(),
            (flow, execution) -> Map.of("trigger", triggerData),
            List.of(
                new Label("tekton.pipelinerun.name", next.getMetadata().getName()),
                new Label("tekton.pipelinerun.status", status)
            ),
            Optional.of(Instant.parse(watermark(next)).atZone(java.time.ZoneOffset.UTC))
        ));
    }

    private boolean terminal(PipelineRun run) {
        return TektonClientSupport.terminal(status(run));
    }

    private String status(PipelineRun run) {
        return TektonClientSupport.conditionStatus(run.getStatus() == null ? null : run.getStatus().getConditions());
    }

    private String watermark(PipelineRun run) {
        if (run.getStatus() != null && run.getStatus().getCompletionTime() != null) {
            return run.getStatus().getCompletionTime();
        }
        return run.getMetadata().getCreationTimestamp();
    }

    private Watermark read(KVStore store, String key) throws Exception {
        return store.getValue(key)
            .map(value -> JacksonMapper.ofJson().convertValue(value.value(), Watermark.class))
            .orElse(new Watermark(null, "1970-01-01T00:00:00Z"));
    }

    private String watermarkKey(TriggerContext triggerContext) {
        String triggerId = triggerContext.getTriggerId() != null ? triggerContext.getTriggerId() : getId();
        return WATERMARK_KEY + "-" + (triggerId == null ? "default" : triggerId);
    }

    private boolean newerThan(PipelineRun run, Watermark watermark) {
        return Instant.parse(watermark(run)).isAfter(Instant.parse(watermark.time()))
            || (Instant.parse(watermark(run)).equals(Instant.parse(watermark.time()))
            && run.getMetadata().getName().compareTo(Optional.ofNullable(watermark.name()).orElse("")) > 0);
    }

    private record Watermark(String name, String time) {
    }
}
