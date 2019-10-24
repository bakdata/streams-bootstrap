[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.common-kafka-streams?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=5&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.common-kafka-streams%3Acommon-kafka-streams&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.common-kafka-streams%3Acommon-kafka-streams)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.common-kafka-streams%3Acommon-kafka-streams&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.common-kafka-streams%3Acommon-kafka-streams)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.common-kafka-streams/common-kafka-streams.svg)](https://search.maven.org/search?q=g:com.bakdata.common-kafka-streams%20AND%20a:common-kafka-streams&core=gav)


# common-kafka-streams

## Getting Started

You can add common-kafka-streams via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata', name: 'common-kafka-streams', version: '1.1.8'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata</groupId>
    <artifactId>common-kafka-streams</artifactId>
    <version>1.1.8</version>
</dependency>
```


For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.common-kafka-streams/common-kafka-streams/latest).

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/common-kafka-streams.git
> cd common-kafka-streams && ./gradlew build
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
Have a look at the [LICENSE](https://github.com/bakdata/common-kafka-streams/blob/master/LICENSE) for more details.
