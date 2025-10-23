package com.bakdata.kafka.mixin;

import static java.util.Collections.emptyMap;

import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import picocli.CommandLine;

@Getter
@Setter
@NoArgsConstructor
public class OutputOptions {
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    private String outputTopic;
    @CommandLine.Option(names = "--labeled-output-topics", split = ",",
            description = "Additional labeled output topics")
    private Map<String, String> labeledOutputTopics = emptyMap();
}
