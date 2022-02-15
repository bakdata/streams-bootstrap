package com.bakdata.kafka.util;

import java.util.Collection;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.jooq.lambda.Seq;

@RequiredArgsConstructor
public class PatternTopicSubscription implements TopicSubscription {
    private final @NonNull Pattern pattern;

    @Override
    public Collection<String> resolveTopics(final Collection<String> allTopics) {
        return Seq.seq(allTopics)
                .filter(this.pattern.asMatchPredicate())
                .toList();
    }
}
