package com.bakdata.kafka.util;

import java.util.Collection;

@FunctionalInterface
public interface TopicSubscription {

    Collection<String> resolveTopics(Collection<String> allTopics);
}
