package io.kestra.plugin.tekton;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
public abstract class AbstractTektonTask extends Task {
    @Schema(
        title = "Kubernetes connection",
        description = "Connection settings for the Kubernetes cluster. When omitted, Fabric8 discovers credentials from system properties, environment variables, kubeconfig, or the in-cluster service account."
    )
    @PluginProperty(group = "connection")
    private Connection connection;

    @Schema(
        title = "Tekton namespace",
        description = "Namespace where the PipelineRun or TaskRun is managed."
    )
    @PluginProperty(group = "connection")
    @NotNull
    @lombok.Builder.Default
    private final Property<String> namespace = Property.ofValue("default");

    @Schema(
        title = "Inherit cluster auto-configuration",
        description = "When true, explicit connection fields override the ambient kubeconfig or in-cluster configuration."
    )
    @PluginProperty(group = "advanced")
    @NotNull
    @lombok.Builder.Default
    private final Property<Boolean> inheritClusterConfig = Property.ofValue(true);

    @Schema(
        title = "Polling interval",
        description = "How often a waiting task checks the run status."
    )
    @PluginProperty(group = "execution")
    @NotNull
    @lombok.Builder.Default
    protected final Property<Duration> pollInterval = Property.ofValue(Duration.ofSeconds(5));

    protected String renderNamespace(RunContext runContext) throws Exception {
        return runContext.render(namespace).as(String.class).orElseThrow();
    }

    protected boolean renderInheritClusterConfig(RunContext runContext) throws Exception {
        return runContext.render(inheritClusterConfig).as(Boolean.class).orElse(true);
    }

    protected Duration renderPollInterval(RunContext runContext) throws Exception {
        Duration value = runContext.render(pollInterval).as(Duration.class).orElse(Duration.ofSeconds(5));
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be greater than PT0S");
        }
        return value;
    }
}
