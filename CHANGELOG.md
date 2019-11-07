# Change Log

## [1.1.12](https://github.com/bakdata/common-kafka-streams/tree/1.1.12) (2019-11-07)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.11...1.1.12)

**Merged pull requests:**

- Fix schema registry clean up [\#35](https://github.com/bakdata/common-kafka-streams/pull/35) ([@philipp94831](https://github.com/philipp94831))

## [1.1.11](https://github.com/bakdata/common-kafka-streams/tree/1.1.11) (2019-11-05)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.10...1.1.11)

**Merged pull requests:**

- Improve code quality [\#34](https://github.com/bakdata/common-kafka-streams/pull/34) ([@philipp94831](https://github.com/philipp94831))
- Add common error handlers [\#33](https://github.com/bakdata/common-kafka-streams/pull/33) ([@philipp94831](https://github.com/philipp94831))

## [1.1.10](https://github.com/bakdata/common-kafka-streams/tree/1.1.10) (2019-10-31)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.9...1.1.10)

**Merged pull requests:**

-  Delete schemas during clean up  [\#32](https://github.com/bakdata/common-kafka-streams/pull/32) ([@torbsto](https://github.com/torbsto))
- Add a helm chart for running Streams App as a job [\#31](https://github.com/bakdata/common-kafka-streams/pull/31) ([@SvenLehmann](https://github.com/SvenLehmann))

## [1.1.9](https://github.com/bakdata/common-kafka-streams/tree/1.1.9) (2019-10-25)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.8...1.1.9)

**Merged pull requests:**

- Delete error topic if requested [\#30](https://github.com/bakdata/common-kafka-streams/pull/30) ([@SvenLehmann](https://github.com/SvenLehmann))

## [1.1.8](https://github.com/bakdata/common-kafka-streams/tree/1.1.8) (2019-10-17)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.7...1.1.8)

**Merged pull requests:**

- Expose delete topic method to subclasses [\#29](https://github.com/bakdata/common-kafka-streams/pull/29) ([@philipp94831](https://github.com/philipp94831))

## [1.1.7](https://github.com/bakdata/common-kafka-streams/tree/1.1.7) (2019-10-16)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.6...1.1.7)

**Merged pull requests:**

- Change reprocessing to clean up [\#26](https://github.com/bakdata/common-kafka-streams/pull/26) ([@torbsto](https://github.com/torbsto))

## [1.1.6](https://github.com/bakdata/common-kafka-streams/tree/1.1.6) (2019-10-11)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.5...1.1.6)

**Merged pull requests:**

- Add utility method for single input topic [\#28](https://github.com/bakdata/common-kafka-streams/pull/28) ([@torbsto](https://github.com/torbsto))

## [1.1.5](https://github.com/bakdata/common-kafka-streams/tree/1.1.5) (2019-10-08)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.4...1.1.5)

**Merged pull requests:**

- Allow multiple input topics and add an error topic [\#27](https://github.com/bakdata/common-kafka-streams/pull/27) ([@torbsto](https://github.com/torbsto))

## [1.1.4](https://github.com/bakdata/common-kafka-streams/tree/1.1.4) (2019-09-11)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.3...1.1.4)

**Closed issues:**

- External stream configuration parameter cannot handle primitive data types other than String [\#23](https://github.com/bakdata/common-kafka-streams/issues/23)

**Merged pull requests:**

- Add hook for registering an uncaught exception handler [\#25](https://github.com/bakdata/common-kafka-streams/pull/25) ([@philipp94831](https://github.com/philipp94831))
- Add rclone chart [\#24](https://github.com/bakdata/common-kafka-streams/pull/24) ([@lawben](https://github.com/lawben))
- Allow kafka streams configuration with external parameter [\#22](https://github.com/bakdata/common-kafka-streams/pull/22) ([@fapaul](https://github.com/fapaul))
- Add JMX prometheus [\#21](https://github.com/bakdata/common-kafka-streams/pull/21) ([@lawben](https://github.com/lawben))

## [1.1.3](https://github.com/bakdata/common-kafka-streams/tree/1.1.3) (2019-08-01)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.2...1.1.3)

**Implemented enhancements:**

- Add CLI parameter to allow reprocessing of data [\#14](https://github.com/bakdata/common-kafka-streams/pull/14) ([@SvenLehmann](https://github.com/SvenLehmann))

**Merged pull requests:**

- Release new version of kafka\-streams chart [\#20](https://github.com/bakdata/common-kafka-streams/pull/20) ([@fapaul](https://github.com/fapaul))
- Change clean up visibility [\#19](https://github.com/bakdata/common-kafka-streams/pull/19) ([@fapaul](https://github.com/fapaul))
- Change reset visibility [\#18](https://github.com/bakdata/common-kafka-streams/pull/18) ([@fapaul](https://github.com/fapaul))
- Initialize topolgy stream before cleanup [\#17](https://github.com/bakdata/common-kafka-streams/pull/17) ([@fapaul](https://github.com/fapaul))
-  Add cleanup possibility on processor startup [\#15](https://github.com/bakdata/common-kafka-streams/pull/15) ([@fapaul](https://github.com/fapaul))
- Fix duplicate in app name [\#16](https://github.com/bakdata/common-kafka-streams/pull/16) ([@lawben](https://github.com/lawben))
- Change log level on debug for child app in different package [\#13](https://github.com/bakdata/common-kafka-streams/pull/13) ([@fapaul](https://github.com/fapaul))
- Add unique AppID method [\#12](https://github.com/bakdata/common-kafka-streams/pull/12) ([@lawben](https://github.com/lawben))
- Log initial configuration on debug [\#11](https://github.com/bakdata/common-kafka-streams/pull/11) ([@fapaul](https://github.com/fapaul))

## [1.1.2](https://github.com/bakdata/common-kafka-streams/tree/1.1.2) (2019-06-27)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.1...1.1.2)

**Merged pull requests:**

- Add support for AWS roles and pod resources [\#10](https://github.com/bakdata/common-kafka-streams/pull/10) ([@SvenLehmann](https://github.com/SvenLehmann))
- Update readme with latest release version number [\#9](https://github.com/bakdata/common-kafka-streams/pull/9) ([@fapaul](https://github.com/fapaul))

## [1.1.1](https://github.com/bakdata/common-kafka-streams/tree/1.1.1) (2019-05-24)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.0...1.1.1)

**Merged pull requests:**

- Add log4j binding for sl4j [\#8](https://github.com/bakdata/common-kafka-streams/pull/8) ([@fapaul](https://github.com/fapaul))
- Set arity for boolean options to 1 to match environment key value pairs [\#7](https://github.com/bakdata/common-kafka-streams/pull/7) ([@fapaul](https://github.com/fapaul))
- Comply default stream application name schema with kube dns [\#6](https://github.com/bakdata/common-kafka-streams/pull/6) ([@fapaul](https://github.com/fapaul))

## [1.1.0](https://github.com/bakdata/common-kafka-streams/tree/1.1.0) (2019-05-14)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.0.1...1.1.0)

**Merged pull requests:**

- Remove default custom environment variables ingestion from values.yaml [\#5](https://github.com/bakdata/common-kafka-streams/pull/5) ([@lawben](https://github.com/lawben))
- Add default log properties [\#4](https://github.com/bakdata/common-kafka-streams/pull/4) ([@lawben](https://github.com/lawben))
- Bakdata Kafka Streams Helm Repository [\#3](https://github.com/bakdata/common-kafka-streams/pull/3) ([@fapaul](https://github.com/fapaul))

## [1.0.1](https://github.com/bakdata/common-kafka-streams/tree/1.0.1) (2019-03-27)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.0.0...1.0.1)


## [1.0.0](https://github.com/bakdata/common-kafka-streams/tree/1.0.0) (2019-03-13)

**Closed issues:**

- Extract common Kafka Streams parts [\#1](https://github.com/bakdata/common-kafka-streams/issues/1)

**Merged pull requests:**

- Kafka streams application [\#2](https://github.com/bakdata/common-kafka-streams/pull/2) ([@fapaul](https://github.com/fapaul))
