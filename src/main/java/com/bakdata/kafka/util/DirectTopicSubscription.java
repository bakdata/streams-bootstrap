package com.bakdata.kafka.util;

import java.util.Collection;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DirectTopicSubscription implements TopicSubscription {
    private final @NonNull Collection<String> topics;

    @Override
    public Collection<String> resolveTopics(final Collection<String> allTopics) {
        return this.topics;
    }
}
