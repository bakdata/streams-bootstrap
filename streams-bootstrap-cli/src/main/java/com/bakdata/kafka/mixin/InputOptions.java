package com.bakdata.kafka.mixin;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.bakdata.kafka.StringListConverter;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import picocli.CommandLine;
import picocli.CommandLine.UseDefaultConverter;

@Getter
@Setter
@NoArgsConstructor
public class InputOptions {
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    private List<String> inputTopics = emptyList();
    @CommandLine.Option(names = "--input-pattern", description = "Input pattern")
    private Pattern inputPattern;
    @CommandLine.Option(names = "--labeled-input-topics", split = ",", description = "Additional labeled input topics",
            converter = {UseDefaultConverter.class, StringListConverter.class})
    private Map<String, List<String>> labeledInputTopics = emptyMap();
    @CommandLine.Option(names = "--labeled-input-patterns", split = ",",
            description = "Additional labeled input patterns")
    private Map<String, Pattern> labeledInputPatterns = emptyMap();
}
