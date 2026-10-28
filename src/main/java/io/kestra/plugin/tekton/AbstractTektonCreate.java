package io.kestra.plugin.tekton;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.tekton.client.TektonClient;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.tekton.model.TektonOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Shared lifecycle for Tekton resources which are created and optionally awaited. */
@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public abstract class AbstractTektonCreate<R extends HasMetadata> extends AbstractTektonTask implements RunnableTask<TektonOutput> {
    @Schema(title = "Wait for completion", description = "When true, wait for the Tekton run to reach a terminal Succeeded condition.")
    @PluginProperty(group = "execution")
    @NotNull
    @Builder.Default
    private final Property<Boolean> wait = Property.ofValue(false);

    @Schema(title = "Wait timeout", description = "Maximum time to wait for the Tekton run to reach a terminal status.")
    @PluginProperty(group = "execution")
    @NotNull
    @Builder.Default
    private final Property<Duration> waitTimeout = Property.ofValue(Duration.ofMinutes(30));

    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean killed = new AtomicBoolean();
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<String> currentName = new AtomicReference<>();
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<String> currentNamespace = new AtomicReference<>();
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<TektonClientSupport.ClientHandle> currentClient = new AtomicReference<>();
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<RunContext> currentRunContext = new AtomicReference<>();

    protected abstract R create(TektonClient client, String namespace, R resource);

    protected abstract Resource<R> resource(TektonClient client, String namespace, String name);

    protected abstract Resource<R> resource(TektonClient client, String namespace, R resource);

    protected abstract void markCancelled(R resource);

    protected abstract TektonOutput output(R resource);

    protected abstract String resourceType();

    protected final TektonOutput execute(RunContext runContext, String namespace, R resource) throws Exception {
        killed.set(false);
        currentNamespace.set(namespace);
        currentRunContext.set(runContext);
        try (TektonClientSupport.ClientHandle client = TektonClientSupport.client(getConnection(), runContext, renderInheritClusterConfig(runContext))) {
            currentClient.set(client);
            R created = create(client.tekton(), namespace, resource);
            String name = created.getMetadata().getName();
            currentName.set(name);
            TektonOutput output = output(created);
            if (!runContext.render(wait).as(Boolean.class).orElse(false)) {
                return output;
            }
            return TektonRunSupport.requireSuccessful(waitForCompletion(runContext, client.tekton(), namespace, name));
        } finally {
            currentClient.set(null);
            currentName.set(null);
            currentNamespace.set(null);
            currentRunContext.set(null);
        }
    }

    private TektonOutput waitForCompletion(RunContext runContext, TektonClient client, String namespace, String name) throws Exception {
        Duration rWaitDuration = runContext.render(waitTimeout).as(Duration.class).orElse(Duration.ofMinutes(30));
        if (rWaitDuration.isZero() || rWaitDuration.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must be greater than PT0S");
        }
        long deadline = System.nanoTime() + rWaitDuration.toNanos();
        while (true) {
            R current = resource(client, namespace, name).get();
            if (current == null) {
                throw new IllegalStateException(resourceType() + " '" + name + "' no longer exists");
            }
            TektonOutput output = output(current);
            if (TektonClientSupport.terminal(output.getStatus())) {
                return output;
            }
            if (System.nanoTime() >= deadline) {
                cancelCurrentRun("wait timeout");
                throw new IllegalStateException("Timed out waiting for " + resourceType() + " '" + name + "'");
            }
            Thread.sleep(renderPollInterval(runContext).toMillis());
        }
    }

    private void cancelCurrentRun(String reason) {
        TektonClientSupport.ClientHandle client = currentClient.get();
        String namespace = currentNamespace.get();
        String name = currentName.get();
        if (client == null || namespace == null || name == null) {
            return;
        }
        try {
            R current = resource(client.tekton(), namespace, name).get();
            if (current != null) {
                // Edit a freshly fetched resource so Fabric8 computes a JSON patch from its resourceVersion.
                resource(client.tekton(), namespace, current).edit(run -> {
                    markCancelled(run);
                    return run;
                });
            }
        } catch (Exception e) {
            RunContext runContext = currentRunContext.get();
            if (runContext != null) {
                runContext.logger().warn("Failed to cancel " + resourceType() + " '" + name + "' after " + reason, e);
            }
        }
    }

    @Override
    public void kill() {
        if (!killed.compareAndSet(false, true)) {
            return;
        }
        cancelCurrentRun("execution cancellation");
    }
}
