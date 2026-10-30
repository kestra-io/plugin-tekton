package io.kestra.plugin.tekton.model;

import io.fabric8.knative.pkg.apis.Condition;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class TektonCondition {
    @Schema(title = "Condition type")
    private final String type;

    @Schema(title = "Condition status")
    private final String status;

    @Schema(title = "Condition reason")
    private final String reason;

    @Schema(title = "Condition message")
    private final String message;

    @Schema(title = "Last transition time")
    private final String lastTransitionTime;

    public static TektonCondition from(Condition condition) {
        return TektonCondition.builder()
            .type(condition.getType())
            .status(condition.getStatus())
            .reason(condition.getReason())
            .message(condition.getMessage())
            .lastTransitionTime(condition.getLastTransitionTime())
            .build();
    }
}
