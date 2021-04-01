[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.streams-bootstrap?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=5&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/streams-bootstrap.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:streams-bootstrap&core=gav)


# streams-bootstrap

`streams-bootstrap` provides base classes and utility functions for Kafka Streams applications.

It provides a common way to
- configure Kafka Streams applications
- deploy streaming applications into a Kafka cluster on Kubernetes via Helm Charts
- reprocess data

Visit our [blogpost](https://medium.com/bakdata/continuous-nlp-pipelines-with-python-java-and-apache-kafka-f6903e7e429d) for an overview and a demo application.  
The common configuration and deployments on Kubernetes are supported by the [Streams-Explorer](https://github.com/bakdata/streams-explorer), which enables to explore and monitor data pipelines in Apache Kafka.

## Getting Started

You can add streams-bootstrap via Maven Central.

#### Gradle

```gradle
compile group: 'com.bakdata.kafka', name: 'streams-bootstrap', version: '1.7.0'
```

#### Maven

```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>streams-bootstrap</artifactId>
    <version>1.7.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/streams-bootstrap/latest).

### Helm Charts

For the configuration and deployment to Kubernetes, you can use the `streams-bootrap` [Helm Charts](https://github.com/bakdata/streams-bootstrap/tree/master/charts).

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/streams-bootstrap.git
> cd streams-bootstrap && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License

This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/streams-bootstrap/blob/master/LICENSE) for more details.
