# Change Log

## [1.5.0](https://github.com/bakdata/common-kafka-streams/tree/1.5.0) (2020-09-29)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.6...1.5.0)

**Merged pull requests:**

- Add labels, annotations and configurable name to rclone CronJob [\#80](https://github.com/bakdata/common-kafka-streams/pull/80) ([@philipp94831](https://github.com/philipp94831))
- Update Kafka to 2.5.1 and Confluent to 5.5.1 [\#79](https://github.com/bakdata/common-kafka-streams/pull/79) ([@philipp94831](https://github.com/philipp94831))

## [1.4.6](https://github.com/bakdata/common-kafka-streams/tree/1.4.6) (2020-08-26)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.5...1.4.6)

**Merged pull requests:**

- Close resources after streams client [\#78](https://github.com/bakdata/common-kafka-streams/pull/78) ([@philipp94831](https://github.com/philipp94831))

## [1.4.5](https://github.com/bakdata/common-kafka-streams/tree/1.4.5) (2020-08-06)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.4...1.4.5)

**Closed issues:**

- Use inheritance for streams helm charts [\#71](https://github.com/bakdata/common-kafka-streams/issues/71)

**Merged pull requests:**

- Make helm chart backwards compatible [\#77](https://github.com/bakdata/common-kafka-streams/pull/77) ([@philipp94831](https://github.com/philipp94831))
- Specify additional output topics via default CLI [\#75](https://github.com/bakdata/common-kafka-streams/pull/75) ([@philipp94831](https://github.com/philipp94831))
- Trim rclone job name [\#76](https://github.com/bakdata/common-kafka-streams/pull/76) ([@philipp94831](https://github.com/philipp94831))
- Add annotations to clean up job [\#74](https://github.com/bakdata/common-kafka-streams/pull/74) ([@philipp94831](https://github.com/philipp94831))
- Remove duplicated streams chart [\#72](https://github.com/bakdata/common-kafka-streams/pull/72) ([@philipp94831](https://github.com/philipp94831))
- Fix yaml file for kubernetes \> 1.16 [\#70](https://github.com/bakdata/common-kafka-streams/pull/70) ([@VictorKuenstler](https://github.com/VictorKuenstler))
- Make JAVA\_TOOL\_OPTIONS configurable [\#68](https://github.com/bakdata/common-kafka-streams/pull/68) ([@philipp94831](https://github.com/philipp94831))

## [1.4.4](https://github.com/bakdata/common-kafka-streams/tree/1.4.4) (2020-04-14)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.3...1.4.4)

**Merged pull requests:**

- Close resources on streams creation error [\#67](https://github.com/bakdata/common-kafka-streams/pull/67) ([@philipp94831](https://github.com/philipp94831))
- Only run streams resetter for existing topics [\#66](https://github.com/bakdata/common-kafka-streams/pull/66) ([@philipp94831](https://github.com/philipp94831))
- Use Kubernetes secrets for password parameters [\#64](https://github.com/bakdata/common-kafka-streams/pull/64) ([@yannick-roeder](https://github.com/yannick-roeder))
- Merge streams chart for statefulset and deployment [\#65](https://github.com/bakdata/common-kafka-streams/pull/65) ([@philipp94831](https://github.com/philipp94831))

## [1.4.3](https://github.com/bakdata/common-kafka-streams/tree/1.4.3) (2020-04-02)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.2...1.4.3)

**Implemented enhancements:**

- Override log4j.properties when using provided [\#62](https://github.com/bakdata/common-kafka-streams/pull/62) ([@BJennWare](https://github.com/BJennWare))

**Merged pull requests:**

- Revert PicoCli api usage to exit application properly [\#63](https://github.com/bakdata/common-kafka-streams/pull/63) ([@philipp94831](https://github.com/philipp94831))

## [1.4.2](https://github.com/bakdata/common-kafka-streams/tree/1.4.2) (2020-03-24)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.1...1.4.2)

**Merged pull requests:**

- Run streams resetter for external source topics and intermediate topics [\#61](https://github.com/bakdata/common-kafka-streams/pull/61) ([@philipp94831](https://github.com/philipp94831))
- Add helm chart to deploy streams app as statefulset with static group membership [\#60](https://github.com/bakdata/common-kafka-streams/pull/60) ([@philipp94831](https://github.com/philipp94831))

## [1.4.1](https://github.com/bakdata/common-kafka-streams/tree/1.4.1) (2020-03-13)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.4.0...1.4.1)

**Merged pull requests:**

- Allow specification of boolean flags without any value [\#59](https://github.com/bakdata/common-kafka-streams/pull/59) ([@philipp94831](https://github.com/philipp94831))
- Add topic client [\#58](https://github.com/bakdata/common-kafka-streams/pull/58) ([@philipp94831](https://github.com/philipp94831))
- Do not exit application [\#57](https://github.com/bakdata/common-kafka-streams/pull/57) ([@philipp94831](https://github.com/philipp94831))

## [1.4.0](https://github.com/bakdata/common-kafka-streams/tree/1.4.0) (2020-03-10)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.3.4...1.4.0)

**Merged pull requests:**

- Remove DeadLetter [\#56](https://github.com/bakdata/common-kafka-streams/pull/56) ([@philipp94831](https://github.com/philipp94831))
- Move error handlers to com.bakdata.kafka:error\-handling [\#55](https://github.com/bakdata/common-kafka-streams/pull/55) ([@philipp94831](https://github.com/philipp94831))

## [1.3.4](https://github.com/bakdata/common-kafka-streams/tree/1.3.4) (2020-03-10)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.3.3...1.3.4)

**Merged pull requests:**

- Parse streams config from separate environment variables [\#54](https://github.com/bakdata/common-kafka-streams/pull/54) ([@philipp94831](https://github.com/philipp94831))

## [1.3.3](https://github.com/bakdata/common-kafka-streams/tree/1.3.3) (2020-02-27)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.3.2...1.3.3)

**Merged pull requests:**

- Exit application with proper code [\#53](https://github.com/bakdata/common-kafka-streams/pull/53) ([@philipp94831](https://github.com/philipp94831))

## [1.3.2](https://github.com/bakdata/common-kafka-streams/tree/1.3.2) (2020-02-24)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.3.1...1.3.2)

**Merged pull requests:**

- Check for errors when running streams resetter [\#52](https://github.com/bakdata/common-kafka-streams/pull/52) ([@philipp94831](https://github.com/philipp94831))

## [1.3.1](https://github.com/bakdata/common-kafka-streams/tree/1.3.1) (2020-01-30)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.3.0...1.3.1)

**Merged pull requests:**

- Add hook for Streams state transitions [\#51](https://github.com/bakdata/common-kafka-streams/pull/51) ([@philipp94831](https://github.com/philipp94831))
- Fix parsing of environment parameters with ENV\_PREFIX in name [\#50](https://github.com/bakdata/common-kafka-streams/pull/50) ([@philipp94831](https://github.com/philipp94831))

## [1.3.0](https://github.com/bakdata/common-kafka-streams/tree/1.3.0) (2020-01-29)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.2.1...1.3.0)

**Merged pull requests:**

- Update Kafka to 2.4.0 [\#49](https://github.com/bakdata/common-kafka-streams/pull/49) ([@philipp94831](https://github.com/philipp94831))

## [1.2.1](https://github.com/bakdata/common-kafka-streams/tree/1.2.1) (2020-01-10)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.2.0...1.2.1)

**Merged pull requests:**

- Configure StreamsResetter with application properties [\#48](https://github.com/bakdata/common-kafka-streams/pull/48) ([@torbsto](https://github.com/torbsto))
- Support schema registry authentication [\#47](https://github.com/bakdata/common-kafka-streams/pull/47) ([@philipp94831](https://github.com/philipp94831))

## [1.2.0](https://github.com/bakdata/common-kafka-streams/tree/1.2.0) (2020-01-08)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.13...1.2.0)

**Implemented enhancements:**

- Call close after application clean up [\#46](https://github.com/bakdata/common-kafka-streams/pull/46) ([@philipp94831](https://github.com/philipp94831))
- Apply overridden Kafka config before CLI passed configuration [\#44](https://github.com/bakdata/common-kafka-streams/issues/44)

**Merged pull requests:**

- Prioritize Kafka Streams Config passed via CLI over overridden properties [\#45](https://github.com/bakdata/common-kafka-streams/pull/45) ([@philipp94831](https://github.com/philipp94831))
- Add custom annotations [\#43](https://github.com/bakdata/common-kafka-streams/pull/43) ([@SvenLehmann](https://github.com/SvenLehmann))
- Improve values.yaml structure [\#42](https://github.com/bakdata/common-kafka-streams/pull/42) ([@SvenLehmann](https://github.com/SvenLehmann))
- Allow custom labels for jobs and deployments [\#41](https://github.com/bakdata/common-kafka-streams/pull/41) ([@SvenLehmann](https://github.com/SvenLehmann))
- Reset internal topics [\#40](https://github.com/bakdata/common-kafka-streams/pull/40) ([@torbsto](https://github.com/torbsto))

## [1.1.13](https://github.com/bakdata/common-kafka-streams/tree/1.1.13) (2019-11-15)
[Full Changelog](https://github.com/bakdata/common-kafka-streams/compare/1.1.12...1.1.13)

**Merged pull requests:**

- Add S3 backed Serde [\#39](https://github.com/bakdata/common-kafka-streams/pull/39) ([@philipp94831](https://github.com/philipp94831))
- Classify all Kafka errors as recoverable bar some exceptions [\#38](https://github.com/bakdata/common-kafka-streams/pull/38) ([@philipp94831](https://github.com/philipp94831))
- Make PodAffinity rule configurable [\#37](https://github.com/bakdata/common-kafka-streams/pull/37) ([@SvenLehmann](https://github.com/SvenLehmann))
- Add flat value transformers for error handling [\#36](https://github.com/bakdata/common-kafka-streams/pull/36) ([@philipp94831](https://github.com/philipp94831))

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
