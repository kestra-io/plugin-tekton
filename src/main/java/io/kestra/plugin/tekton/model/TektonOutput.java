package io.kestra.plugin.tekton.model;

import io.fabric8.knative.pkg.apis.Condition;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.kestra.plugin.tekton.TektonClientSupport;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class TektonOutput implements io.kestra.core.models.tasks.Output {
    @Schema(title = "Run name")
    private final String name;

    @Schema(title = "Run namespace")
    private final String namespace;

    @Schema(title = "Run status", description = "True, False, or Unknown from the Tekton Succeeded condition.")
    private final String status;

    @Schema(title = "Run conditions")
    private final List<TektonCondition> conditions;

    @Schema(title = "Run start time")
    private final Instant startTime;

    @Schema(title = "Run completion time")
    private final Instant completionTime;

    @Schema(title = "Run results")
    private final Map<String, String> results;

    public static TektonOutput from(ObjectMeta metadata, List<Condition> conditions, String startTime, String completionTime, List<?> results) {
        return TektonOutput.builder()
            .name(metadata == null ? null : metadata.getName())
            .namespace(metadata == null ? null : metadata.getNamespace())
            .status(TektonClientSupport.conditionStatus(conditions))
            .conditions(conditions == null ? List.of() : conditions.stream().map(TektonCondition::from).toList())
            .startTime(TektonClientSupport.instant(startTime))
            .completionTime(TektonClientSupport.instant(completionTime))
            .results(TektonClientSupport.results(results))
            .build();
    }
}
