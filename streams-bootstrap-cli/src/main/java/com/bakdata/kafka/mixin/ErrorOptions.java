package com.bakdata.kafka.mixin;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import picocli.CommandLine;

@Getter
@Setter
@NoArgsConstructor
public class ErrorOptions {
    @CommandLine.Option(names = "--error-topic", description = "Error topic")
    private String errorTopic;
}
