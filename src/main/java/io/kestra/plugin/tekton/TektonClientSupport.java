package io.kestra.plugin.tekton;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.knative.pkg.apis.Condition;
import io.fabric8.tekton.v1.ParamValue;
import io.fabric8.tekton.client.TektonClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.shared.models.Connection;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TektonClientSupport {
    private TektonClientSupport() {
    }

    public static ClientHandle client(Connection connection, RunContext runContext, boolean inheritClusterConfig)
        throws IllegalVariableEvaluationException {
        KubernetesClient kubernetesClient;
        if (connection == null) {
            kubernetesClient = new KubernetesClientBuilder().build();
        } else {
            Config config = connection.toConfig(runContext, inheritClusterConfig);
            kubernetesClient = new KubernetesClientBuilder().withConfig(config).build();
        }
        return new ClientHandle(kubernetesClient, kubernetesClient.adapt(TektonClient.class));
    }

    public static String conditionStatus(List<? extends Condition> conditions) {
        if (conditions == null) {
            return "Unknown";
        }
        return conditions.stream()
            .filter(condition -> "Succeeded".equals(condition.getType()))
            .map(Condition::getStatus)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse("Unknown");
    }

    public static boolean terminal(String status) {
        return "True".equals(status) || "False".equals(status);
    }

    public static Instant instant(String value) {
        return value == null ? null : Instant.parse(value);
    }

    public static Map<String, String> results(List<? extends Object> results) {
        Map<String, String> output = new LinkedHashMap<>();
        if (results == null) {
            return output;
        }
        for (Object result : results) {
            Map<String, Object> values = JacksonMapper.toMap(result);
            String name = String.valueOf(values.get("name"));
            Object value = values.get("value");
            if (value != null) {
                try {
                    output.put(name, resultValue(value));
                } catch (JsonProcessingException e) {
                    output.put(name, String.valueOf(value));
                }
            }
        }
        return output;
    }

    private static String resultValue(Object value) throws JsonProcessingException {
        if (value instanceof String string) {
            return string;
        }
        if (value instanceof ParamValue paramValue && "string".equals(paramValue.getType())) {
            return paramValue.getStringVal();
        }
        return JacksonMapper.ofJson().writeValueAsString(value);
    }

    public record ClientHandle(KubernetesClient kubernetes, TektonClient tekton) implements AutoCloseable {
        @Override
        public void close() {
            kubernetes.close();
        }
    }
}
