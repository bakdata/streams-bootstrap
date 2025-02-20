# Change Log

## [3.5.2](https://github.com/bakdata/streams-bootstrap/tree/3.5.2) (2025-02-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.5.2...3.5.2)


## [3.5.2](https://github.com/bakdata/streams-bootstrap/tree/3.5.2) (2025-02-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.5.1...3.5.2)

**Merged pull requests:**

- Retry topic verification [\#278](https://github.com/bakdata/streams-bootstrap/pull/278) ([@philipp94831](https://github.com/philipp94831))

## [3.5.1](https://github.com/bakdata/streams-bootstrap/tree/3.5.1) (2025-01-23)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.5.0...3.5.1)

**Closed issues:**

- Close method is called twice on shutdown [\#263](https://github.com/bakdata/streams-bootstrap/issues/263)

**Merged pull requests:**

- Call close on KafkaApplication only once [\#275](https://github.com/bakdata/streams-bootstrap/pull/275) ([@philipp94831](https://github.com/philipp94831))

## [3.5.0](https://github.com/bakdata/streams-bootstrap/tree/3.5.0) (2025-01-20)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.4.0...3.5.0)

**Merged pull requests:**

- Run tests in parallel [\#274](https://github.com/bakdata/streams-bootstrap/pull/274) ([@philipp94831](https://github.com/philipp94831))
- Use Awaitility [\#273](https://github.com/bakdata/streams-bootstrap/pull/273) ([@philipp94831](https://github.com/philipp94831))
- Add methods for simplified testing of Kafka endpoints [\#270](https://github.com/bakdata/streams-bootstrap/pull/270) ([@philipp94831](https://github.com/philipp94831))
- Fix Helm publish [\#271](https://github.com/bakdata/streams-bootstrap/pull/271) ([@yannick-roeder](https://github.com/yannick-roeder))

## [3.4.0](https://github.com/bakdata/streams-bootstrap/tree/3.4.0) (2025-01-09)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.3.0...3.4.0)

**Merged pull requests:**

- Use Confluent MockSchemaRegistry [\#269](https://github.com/bakdata/streams-bootstrap/pull/269) ([@philipp94831](https://github.com/philipp94831))
- Upgrade large\-message to 2.9 [\#268](https://github.com/bakdata/streams-bootstrap/pull/268) ([@philipp94831](https://github.com/philipp94831))

## [3.3.0](https://github.com/bakdata/streams-bootstrap/tree/3.3.0) (2025-01-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.2.0...3.3.0)

**Merged pull requests:**

- Upgrade to Kafka 3.8 [\#267](https://github.com/bakdata/streams-bootstrap/pull/267) ([@philipp94831](https://github.com/philipp94831))

## [3.2.0](https://github.com/bakdata/streams-bootstrap/tree/3.2.0) (2025-01-07)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.1.0...3.2.0)

**Merged pull requests:**

- Upgrade to Kafka 3.7 [\#266](https://github.com/bakdata/streams-bootstrap/pull/266) ([@philipp94831](https://github.com/philipp94831))
- Replace kafka\-junit with testcontainers [\#264](https://github.com/bakdata/streams-bootstrap/pull/264) ([@philipp94831](https://github.com/philipp94831))
- Fix version in README [\#262](https://github.com/bakdata/streams-bootstrap/pull/262) ([@philipp94831](https://github.com/philipp94831))

## [3.1.0](https://github.com/bakdata/streams-bootstrap/tree/3.1.0) (2024-12-05)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.0.4...3.1.0)

**Merged pull requests:**

- Retain type of Streams/ProducerApp in CLI module [\#261](https://github.com/bakdata/streams-bootstrap/pull/261) ([@philipp94831](https://github.com/philipp94831))

## [3.0.4](https://github.com/bakdata/streams-bootstrap/tree/3.0.4) (2024-12-03)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.0.3...3.0.4)

**Merged pull requests:**

- Do not configure replicas if autoscaling is enabled [\#260](https://github.com/bakdata/streams-bootstrap/pull/260) ([@philipp94831](https://github.com/philipp94831))

## [3.0.3](https://github.com/bakdata/streams-bootstrap/tree/3.0.3) (2024-11-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.0.2...3.0.3)

**Merged pull requests:**

- Fix producer app name template [\#259](https://github.com/bakdata/streams-bootstrap/pull/259) ([@raminqaf](https://github.com/raminqaf))

## [3.0.2](https://github.com/bakdata/streams-bootstrap/tree/3.0.2) (2024-10-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/3.0.1...3.0.2)

**Merged pull requests:**

- Add annotations to all Kubernetes resources [\#258](https://github.com/bakdata/streams-bootstrap/pull/258) ([@raminqaf](https://github.com/raminqaf))

## [3.0.1](https://github.com/bakdata/streams-bootstrap/tree/3.0.1) (2024-08-20)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.23.0...3.0.1)

**Closed issues:**

- Change default productive  to false [\#151](https://github.com/bakdata/streams-bootstrap/issues/151)
- CloseOptions supports timeout [\#189](https://github.com/bakdata/streams-bootstrap/issues/189)
- Rename `\-\-brokers` to `\-\-bootstrap\-server` [\#232](https://github.com/bakdata/streams-bootstrap/issues/232)
- Remove avro dependency [\#235](https://github.com/bakdata/streams-bootstrap/issues/235)
- Rename extra\-input/output topic to named\-input/output topic [\#231](https://github.com/bakdata/streams-bootstrap/issues/231)
- Remove guava dependency [\#236](https://github.com/bakdata/streams-bootstrap/issues/236)
- Remove dependency on log4j [\#162](https://github.com/bakdata/streams-bootstrap/issues/162)
- Separate rclone chart from streams\-bootstrap [\#229](https://github.com/bakdata/streams-bootstrap/issues/229)
- Investigate the necessity of `nameOverride` [\#185](https://github.com/bakdata/streams-bootstrap/issues/185)
- Distinguish pod and deployment annotations in Helm charts [\#171](https://github.com/bakdata/streams-bootstrap/issues/171)
- Migrate from Azure Pipelines to GitHub Actions [\#139](https://github.com/bakdata/streams-bootstrap/issues/139)

**Merged pull requests:**

- Create v3 [\#250](https://github.com/bakdata/streams-bootstrap/pull/250) ([@philipp94831](https://github.com/philipp94831))
- Pre\-bump version to 3.0.1\-SNAPSHOT [\#257](https://github.com/bakdata/streams-bootstrap/pull/257) ([@philipp94831](https://github.com/philipp94831))
- Add hook to prepare running of app [\#256](https://github.com/bakdata/streams-bootstrap/pull/256) ([@philipp94831](https://github.com/philipp94831))
- Validate persistence mandatory chart values \(persistence.size\) [\#255](https://github.com/bakdata/streams-bootstrap/pull/255) ([@raminqaf](https://github.com/raminqaf))
- Validate autoscaling mandatory fields when it is enabled [\#254](https://github.com/bakdata/streams-bootstrap/pull/254) ([@raminqaf](https://github.com/raminqaf))
- Fix Sonarqube issues [\#253](https://github.com/bakdata/streams-bootstrap/pull/253) ([@philipp94831](https://github.com/philipp94831))
- Rename Streams section in Helm chart docs [\#252](https://github.com/bakdata/streams-bootstrap/pull/252) ([@philipp94831](https://github.com/philipp94831))
- Make CleanUpRunner closeable [\#247](https://github.com/bakdata/streams-bootstrap/pull/247) ([@philipp94831](https://github.com/philipp94831))
- Rename TestTopologyFactory [\#249](https://github.com/bakdata/streams-bootstrap/pull/249) ([@philipp94831](https://github.com/philipp94831))
- Replace Guava usages [\#246](https://github.com/bakdata/streams-bootstrap/pull/246) ([@philipp94831](https://github.com/philipp94831))
- Remove unnecessary schema registry configurations in tests [\#248](https://github.com/bakdata/streams-bootstrap/pull/248) ([@philipp94831](https://github.com/philipp94831))
- Add CLI parameter to specify streams application ID [\#243](https://github.com/bakdata/streams-bootstrap/pull/243) ([@philipp94831](https://github.com/philipp94831))
- Rename `\-\-brokers` to `\-\-bootstrap\-servers` [\#242](https://github.com/bakdata/streams-bootstrap/pull/242) ([@philipp94831](https://github.com/philipp94831))
- Rename extra topics to labeled topics [\#240](https://github.com/bakdata/streams-bootstrap/pull/240) ([@philipp94831](https://github.com/philipp94831))
- Rename streams section to kafka in Helm charts [\#241](https://github.com/bakdata/streams-bootstrap/pull/241) ([@philipp94831](https://github.com/philipp94831))
- Add default serialization config to apps [\#239](https://github.com/bakdata/streams-bootstrap/pull/239) ([@philipp94831](https://github.com/philipp94831))
- Remove log4j dependency and debug parameter [\#238](https://github.com/bakdata/streams-bootstrap/pull/238) ([@philipp94831](https://github.com/philipp94831))
- Add HostInfo to ImprovedStreamsConfig [\#230](https://github.com/bakdata/streams-bootstrap/pull/230) ([@raminqaf](https://github.com/raminqaf))
- Use KAFKA\_ as prefix for environment Kafka config [\#209](https://github.com/bakdata/streams-bootstrap/pull/209) ([@philipp94831](https://github.com/philipp94831))
- Remove guava dependency [\#237](https://github.com/bakdata/streams-bootstrap/pull/237) ([@philipp94831](https://github.com/philipp94831))
- Separate CLI and application [\#207](https://github.com/bakdata/streams-bootstrap/pull/207) ([@philipp94831](https://github.com/philipp94831))
- feat: remove rclone [\#234](https://github.com/bakdata/streams-bootstrap/pull/234) ([@DerTiedemann](https://github.com/DerTiedemann))
- fix: add gh actions badge [\#233](https://github.com/bakdata/streams-bootstrap/pull/233) ([@DerTiedemann](https://github.com/DerTiedemann))

## [2.23.0](https://github.com/bakdata/streams-bootstrap/tree/2.23.0) (2024-06-12)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.22.1...2.23.0)

**Merged pull requests:**

- Add dynamic application.server config to streams app chart [\#214](https://github.com/bakdata/streams-bootstrap/pull/214) ([@philipp94831](https://github.com/philipp94831))

## [2.22.1](https://github.com/bakdata/streams-bootstrap/tree/2.22.1) (2024-06-11)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.22.0...2.22.1)

**Merged pull requests:**

- Use same variable names for clean up job as for main chart [\#228](https://github.com/bakdata/streams-bootstrap/pull/228) ([@philipp94831](https://github.com/philipp94831))

## [2.22.0](https://github.com/bakdata/streams-bootstrap/tree/2.22.0) (2024-06-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.21.0...2.22.0)

**Merged pull requests:**

- Support mounting of external secrets [\#225](https://github.com/bakdata/streams-bootstrap/pull/225) ([@torbsto](https://github.com/torbsto))

## [2.21.0](https://github.com/bakdata/streams-bootstrap/tree/2.21.0) (2024-05-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.20.1...2.21.0)

**Merged pull requests:**

- Make affinity in charts more generic [\#224](https://github.com/bakdata/streams-bootstrap/pull/224) ([@yannick-roeder](https://github.com/yannick-roeder))

## [2.20.1](https://github.com/bakdata/streams-bootstrap/tree/2.20.1) (2024-05-23)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.20.0...2.20.1)

**Merged pull requests:**

- Roll deployments on config map updates [\#227](https://github.com/bakdata/streams-bootstrap/pull/227) ([@philipp94831](https://github.com/philipp94831))
- Fix content of config maps [\#226](https://github.com/bakdata/streams-bootstrap/pull/226) ([@philipp94831](https://github.com/philipp94831))

## [2.20.0](https://github.com/bakdata/streams-bootstrap/tree/2.20.0) (2024-05-23)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.19.2...2.20.0)

**Merged pull requests:**

- Mount arbitrary files to apps [\#223](https://github.com/bakdata/streams-bootstrap/pull/223) ([@philipp94831](https://github.com/philipp94831))

## [2.19.2](https://github.com/bakdata/streams-bootstrap/tree/2.19.2) (2024-05-14)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.19.1...2.19.2)

**Merged pull requests:**

- Fix indentation of Rclone annotations [\#222](https://github.com/bakdata/streams-bootstrap/pull/222) ([@philipp94831](https://github.com/philipp94831))
- Bump CI template version to fix Helm lint [\#221](https://github.com/bakdata/streams-bootstrap/pull/221) ([@yannick-roeder](https://github.com/yannick-roeder))

## [2.19.1](https://github.com/bakdata/streams-bootstrap/tree/2.19.1) (2024-05-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.19.0...2.19.1)

**Merged pull requests:**

- Fix default values for podLabels and podAnnotations [\#220](https://github.com/bakdata/streams-bootstrap/pull/220) ([@philipp94831](https://github.com/philipp94831))

## [2.19.0](https://github.com/bakdata/streams-bootstrap/tree/2.19.0) (2024-05-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.18.1...2.19.0)

**Merged pull requests:**

- Remove awsRole from helm charts [\#219](https://github.com/bakdata/streams-bootstrap/pull/219) ([@philipp94831](https://github.com/philipp94831))
- Add Values.podLabels and Values.podAnnotations [\#217](https://github.com/bakdata/streams-bootstrap/pull/217) ([@vostres](https://github.com/vostres))
- Fix naming of helm releases [\#216](https://github.com/bakdata/streams-bootstrap/pull/216) ([@philipp94831](https://github.com/philipp94831))

## [2.18.1](https://github.com/bakdata/streams-bootstrap/tree/2.18.1) (2024-05-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.18.0...2.18.1)

**Merged pull requests:**

- Shorten log4j\-config\-volume to log4j [\#218](https://github.com/bakdata/streams-bootstrap/pull/218) ([@raminqaf](https://github.com/raminqaf))
- Add terminationGracePeriodSeconds [\#212](https://github.com/bakdata/streams-bootstrap/pull/212) ([@vostres](https://github.com/vostres))
- Prevent closing of reused admin clients [\#213](https://github.com/bakdata/streams-bootstrap/pull/213) ([@philipp94831](https://github.com/philipp94831))
- Upgrade fluent kafka streams to 2.13.1 [\#215](https://github.com/bakdata/streams-bootstrap/pull/215) ([@philipp94831](https://github.com/philipp94831))
- Publish snapshots for Helm charts [\#211](https://github.com/bakdata/streams-bootstrap/pull/211) ([@philipp94831](https://github.com/philipp94831))
- Update CI templates [\#210](https://github.com/bakdata/streams-bootstrap/pull/210) ([@philipp94831](https://github.com/philipp94831))

## [2.18.0](https://github.com/bakdata/streams-bootstrap/tree/2.18.0) (2024-04-17)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.17.0...2.18.0)

**Merged pull requests:**

- Bump fluent\-kafka\-streams\-tests [\#208](https://github.com/bakdata/streams-bootstrap/pull/208) ([@JakobEdding](https://github.com/JakobEdding))

## [2.17.0](https://github.com/bakdata/streams-bootstrap/tree/2.17.0) (2024-03-01)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.16.1...2.17.0)

**Merged pull requests:**

- Upgrade to Kafka 3.6 [\#206](https://github.com/bakdata/streams-bootstrap/pull/206) ([@philipp94831](https://github.com/philipp94831))
- Migrate azure pipeline to GH\-action [\#191](https://github.com/bakdata/streams-bootstrap/pull/191) ([@MichaelKora](https://github.com/MichaelKora))
- Improve CI caching [\#205](https://github.com/bakdata/streams-bootstrap/pull/205) ([@philipp94831](https://github.com/philipp94831))
- Upgrade to Gradle 8.4 [\#204](https://github.com/bakdata/streams-bootstrap/pull/204) ([@philipp94831](https://github.com/philipp94831))

## [2.16.1](https://github.com/bakdata/streams-bootstrap/tree/2.16.1) (2024-02-02)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.16.0...2.16.1)

**Closed issues:**

- Fix secretRefs for cleanup job [\#202](https://github.com/bakdata/streams-bootstrap/issues/202)

**Merged pull requests:**

- Fix secretRefs for cleanup jobs [\#203](https://github.com/bakdata/streams-bootstrap/pull/203) ([@mkcode92](https://github.com/mkcode92))
- Fix Jacoco for Java 17 [\#201](https://github.com/bakdata/streams-bootstrap/pull/201) ([@philipp94831](https://github.com/philipp94831))

## [2.16.0](https://github.com/bakdata/streams-bootstrap/tree/2.16.0) (2024-01-26)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.15.3...2.16.0)

**Merged pull requests:**

- Add missing default values for internal topics [\#200](https://github.com/bakdata/streams-bootstrap/pull/200) ([@philipp94831](https://github.com/philipp94831))
- Simplify configuration of internal topics for autoscaling [\#198](https://github.com/bakdata/streams-bootstrap/pull/198) ([@philipp94831](https://github.com/philipp94831))
- Build in CI using JDK 17 [\#199](https://github.com/bakdata/streams-bootstrap/pull/199) ([@philipp94831](https://github.com/philipp94831))

## [2.15.3](https://github.com/bakdata/streams-bootstrap/tree/2.15.3) (2024-01-09)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.15.2...2.15.3)

**Merged pull requests:**

- Update dependencies [\#196](https://github.com/bakdata/streams-bootstrap/pull/196) ([@philipp94831](https://github.com/philipp94831))

## [2.15.2](https://github.com/bakdata/streams-bootstrap/tree/2.15.2) (2024-01-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.15.1...2.15.2)

**Merged pull requests:**

- Update dependencies [\#195](https://github.com/bakdata/streams-bootstrap/pull/195) ([@philipp94831](https://github.com/philipp94831))

## [2.15.1](https://github.com/bakdata/streams-bootstrap/tree/2.15.1) (2023-12-20)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.15.0...2.15.1)

**Merged pull requests:**

- Add slf4j dependency [\#194](https://github.com/bakdata/streams-bootstrap/pull/194) ([@philipp94831](https://github.com/philipp94831))

## [2.15.0](https://github.com/bakdata/streams-bootstrap/tree/2.15.0) (2023-11-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.14.0...2.15.0)

**Merged pull requests:**

- Upgrade to Kafka 3.5 [\#192](https://github.com/bakdata/streams-bootstrap/pull/192) ([@philipp94831](https://github.com/philipp94831))

## [2.14.0](https://github.com/bakdata/streams-bootstrap/tree/2.14.0) (2023-10-30)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.13.0...2.14.0)

**Merged pull requests:**

- Add modules for easy usage with Fluent Kafka Streams Tests and Large Message SerDe [\#188](https://github.com/bakdata/streams-bootstrap/pull/188) ([@philipp94831](https://github.com/philipp94831))

## [2.13.0](https://github.com/bakdata/streams-bootstrap/tree/2.13.0) (2023-09-21)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.12.1...2.13.0)

**Merged pull requests:**

- Make Schema Registry URL optional [\#187](https://github.com/bakdata/streams-bootstrap/pull/187) ([@raminqaf](https://github.com/raminqaf))

**Fixed bugs:**

- Cleanup job fails if topic doesn't have associated schema [\#186](https://github.com/bakdata/streams-bootstrap/issues/186)

## [2.12.1](https://github.com/bakdata/streams-bootstrap/tree/2.12.1) (2023-08-15)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.12.0...2.12.1)

**Closed issues:**

- Upgrade to Kafka 3.4 [\#172](https://github.com/bakdata/streams-bootstrap/issues/172)

**Merged pull requests:**

- Switch to official Rclone image [\#176](https://github.com/bakdata/streams-bootstrap/pull/176) ([@philipp94831](https://github.com/philipp94831))
- Upgrade to Kafka 3.4 [\#184](https://github.com/bakdata/streams-bootstrap/pull/184) ([@torbsto](https://github.com/torbsto))

## [2.12.0](https://github.com/bakdata/streams-bootstrap/tree/2.12.0) (2023-05-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.11.2...2.12.0)

**Merged pull requests:**

- Create method to register topic clean up hooks [\#183](https://github.com/bakdata/streams-bootstrap/pull/183) ([@philipp94831](https://github.com/philipp94831))
- Fail streams\-app templating if no topics are provided for ScaledObject triggers [\#182](https://github.com/bakdata/streams-bootstrap/pull/182) ([@JakobEdding](https://github.com/JakobEdding))

## [2.11.2](https://github.com/bakdata/streams-bootstrap/tree/2.11.2) (2023-04-19)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.11.1...2.11.2)

**Merged pull requests:**

- Make brokers and schema registry url optional chart configs [\#181](https://github.com/bakdata/streams-bootstrap/pull/181) ([@philipp94831](https://github.com/philipp94831))

## [2.11.1](https://github.com/bakdata/streams-bootstrap/tree/2.11.1) (2023-04-17)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.11.0...2.11.1)

**Fixed bugs:**

- Fix service selector to match pod labels [\#180](https://github.com/bakdata/streams-bootstrap/pull/180) ([@raminqaf](https://github.com/raminqaf))

## [2.11.0](https://github.com/bakdata/streams-bootstrap/tree/2.11.0) (2023-04-14)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.10.0...2.11.0)

**Merged pull requests:**

- Reference existing secrets from helm charts [\#179](https://github.com/bakdata/streams-bootstrap/pull/179) ([@philipp94831](https://github.com/philipp94831))
- Support deployment of bootstrap application with an HTTP server [\#178](https://github.com/bakdata/streams-bootstrap/pull/178) ([@torbsto](https://github.com/torbsto))
- Support additional triggers for KEDA autoscaling [\#177](https://github.com/bakdata/streams-bootstrap/pull/177) ([@torbsto](https://github.com/torbsto))

## [2.10.0](https://github.com/bakdata/streams-bootstrap/tree/2.10.0) (2023-03-15)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.9.0...2.10.0)

**Merged pull requests:**

- Producer App can be deployed as k8s Deployment [\#175](https://github.com/bakdata/streams-bootstrap/pull/175) ([@mkcode92](https://github.com/mkcode92))
- Remove duplicate of changelog config [\#174](https://github.com/bakdata/streams-bootstrap/pull/174) ([@yannick-roeder](https://github.com/yannick-roeder))

## [2.9.0](https://github.com/bakdata/streams-bootstrap/tree/2.9.0) (2023-02-21)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.8.0...2.9.0)

**Merged pull requests:**

- Fix casing of consumerGroup value [\#173](https://github.com/bakdata/streams-bootstrap/pull/173) ([@disrupted](https://github.com/disrupted))

## [2.8.0](https://github.com/bakdata/streams-bootstrap/tree/2.8.0) (2023-01-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.7.0...2.8.0)

**Merged pull requests:**

- Upgrade to Kafka 3.3.2 [\#169](https://github.com/bakdata/streams-bootstrap/pull/169) ([@philipp94831](https://github.com/philipp94831))

## [2.7.0](https://github.com/bakdata/streams-bootstrap/tree/2.7.0) (2023-01-03)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.6.0...2.7.0)

**Closed issues:**

- Support new KafkaStreams close options [\#163](https://github.com/bakdata/streams-bootstrap/issues/163)

**Merged pull requests:**

- Update dependencies [\#168](https://github.com/bakdata/streams-bootstrap/pull/168) ([@philipp94831](https://github.com/philipp94831))
- Improve backwards compatibility with 2.5 and older versions [\#167](https://github.com/bakdata/streams-bootstrap/pull/167) ([@philipp94831](https://github.com/philipp94831))
- Send leave group requests in case of streams application shutdown [\#165](https://github.com/bakdata/streams-bootstrap/pull/165) ([@philipp94831](https://github.com/philipp94831))
- Fix Helm lint [\#166](https://github.com/bakdata/streams-bootstrap/pull/166) ([@philipp94831](https://github.com/philipp94831))
- Use environment config when creating admin client [\#164](https://github.com/bakdata/streams-bootstrap/pull/164) ([@philipp94831](https://github.com/philipp94831))

## [2.6.0](https://github.com/bakdata/streams-bootstrap/tree/2.6.0) (2022-12-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.5.0...2.6.0)

**Closed issues:**

- Missing CLI arguments in clean job don't crash the app [\#105](https://github.com/bakdata/streams-bootstrap/issues/105)

**Merged pull requests:**

- Use new CommandLine API [\#161](https://github.com/bakdata/streams-bootstrap/pull/161) ([@philipp94831](https://github.com/philipp94831))
- Use the latest version of the workflow [\#160](https://github.com/bakdata/streams-bootstrap/pull/160) ([@MichaelKora](https://github.com/MichaelKora))

## [2.5.0](https://github.com/bakdata/streams-bootstrap/tree/2.5.0) (2022-12-22)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.4.2...2.5.0)

**Merged pull requests:**

- Upgrade to Kafka 3.3 [\#157](https://github.com/bakdata/streams-bootstrap/pull/157) ([@philipp94831](https://github.com/philipp94831))

## [2.4.2](https://github.com/bakdata/streams-bootstrap/tree/2.4.2) (2022-12-21)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.4.1...2.4.2)

**Closed issues:**

- Release helm charts to GitHub Pages [\#140](https://github.com/bakdata/streams-bootstrap/issues/140)
- Add optional ServiceAccount to Pod specs [\#154](https://github.com/bakdata/streams-bootstrap/issues/154)

**Merged pull requests:**

- Lint and release helm charts [\#158](https://github.com/bakdata/streams-bootstrap/pull/158) ([@MichaelKora](https://github.com/MichaelKora))
- Remove tgz files and index.yaml [\#159](https://github.com/bakdata/streams-bootstrap/pull/159) ([@MichaelKora](https://github.com/MichaelKora))
- Add ServiceAccountName option to Helm charts [\#156](https://github.com/bakdata/streams-bootstrap/pull/156) ([@yannick-roeder](https://github.com/yannick-roeder))
- Configure access token for changelog generation [\#155](https://github.com/bakdata/streams-bootstrap/pull/155) ([@philipp94831](https://github.com/philipp94831))

## [2.4.1](https://github.com/bakdata/streams-bootstrap/tree/2.4.1) (2022-11-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.4.0...2.4.1)

**Merged pull requests:**

- Clean schema registry subjects of named foreign key joins [\#153](https://github.com/bakdata/streams-bootstrap/pull/153) ([@philipp94831](https://github.com/philipp94831))
- Do not configure empty input topics [\#152](https://github.com/bakdata/streams-bootstrap/pull/152) ([@philipp94831](https://github.com/philipp94831))

**Fixed bugs:**

- Chart should not set inputTopics to empty string if none are specified [\#149](https://github.com/bakdata/streams-bootstrap/issues/149)

## [2.4.0](https://github.com/bakdata/streams-bootstrap/tree/2.4.0) (2022-11-16)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.3.1...2.4.0)

**Merged pull requests:**

- Support multiple extra input topics [\#150](https://github.com/bakdata/streams-bootstrap/pull/150) ([@philipp94831](https://github.com/philipp94831))

## [2.3.1](https://github.com/bakdata/streams-bootstrap/tree/2.3.1) (2022-10-12)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.3.0...2.3.1)

**Merged pull requests:**

- Fix discovery of repartition topics [\#148](https://github.com/bakdata/streams-bootstrap/pull/148) ([@philipp94831](https://github.com/philipp94831))
- Make input topic accessors public [\#147](https://github.com/bakdata/streams-bootstrap/pull/147) ([@philipp94831](https://github.com/philipp94831))

## [2.3.0](https://github.com/bakdata/streams-bootstrap/tree/2.3.0) (2022-07-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.2.0...2.3.0)

**Merged pull requests:**

- Upgrade to Kafka 3.2 and Confluent 7.2 [\#146](https://github.com/bakdata/streams-bootstrap/pull/146) ([@philipp94831](https://github.com/philipp94831))

## [2.2.0](https://github.com/bakdata/streams-bootstrap/tree/2.2.0) (2022-06-23)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.1.2...2.2.0)

**Merged pull requests:**

- Upgrade to Kafka 3.1 and Confluent 7.1 [\#145](https://github.com/bakdata/streams-bootstrap/pull/145) ([@philipp94831](https://github.com/philipp94831))

## [2.1.2](https://github.com/bakdata/streams-bootstrap/tree/2.1.2) (2022-06-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.1.1...2.1.2)

**Merged pull requests:**

- Update documentation [\#144](https://github.com/bakdata/streams-bootstrap/pull/144) ([@philipp94831](https://github.com/philipp94831))
- Remove default schedule for producer jobs [\#143](https://github.com/bakdata/streams-bootstrap/pull/143) ([@philipp94831](https://github.com/philipp94831))
- Fix key\-value pairs indentation inside pod.yaml file [\#141](https://github.com/bakdata/streams-bootstrap/pull/141) ([@irux](https://github.com/irux))
- Update CronJob apiVersion due to deprecation [\#142](https://github.com/bakdata/streams-bootstrap/pull/142) ([@JakobEdding](https://github.com/JakobEdding))

## [2.1.1](https://github.com/bakdata/streams-bootstrap/tree/2.1.1) (2022-03-02)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.1.0...2.1.1)

**Closed issues:**

- Clean up should check if topic deletion was successful [\#134](https://github.com/bakdata/streams-bootstrap/issues/134)

**Merged pull requests:**

- Add more configurations to jobs and cron jobs [\#136](https://github.com/bakdata/streams-bootstrap/pull/136) ([@philipp94831](https://github.com/philipp94831))
- Verify deletion of topics [\#135](https://github.com/bakdata/streams-bootstrap/pull/135) ([@philipp94831](https://github.com/philipp94831))

## [2.1.0](https://github.com/bakdata/streams-bootstrap/tree/2.1.0) (2022-02-21)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/2.0.0...2.1.0)

**Merged pull requests:**

- Add input pattern config [\#132](https://github.com/bakdata/streams-bootstrap/pull/132) ([@philipp94831](https://github.com/philipp94831))
- Add imagePullSecrets to streams\-app helm chart [\#133](https://github.com/bakdata/streams-bootstrap/pull/133) ([@raminqaf](https://github.com/raminqaf))
- Fix clean for pattern subscriptions [\#131](https://github.com/bakdata/streams-bootstrap/pull/131) ([@philipp94831](https://github.com/philipp94831))
- Add option for streams state persistence [\#130](https://github.com/bakdata/streams-bootstrap/pull/130) ([@philipp94831](https://github.com/philipp94831))

## [2.0.0](https://github.com/bakdata/streams-bootstrap/tree/2.0.0) (2022-01-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.9.3...2.0.0)

**Closed issues:**

- Upgrade Kafka Streams to 3.0 [\#126](https://github.com/bakdata/streams-bootstrap/issues/126)

**Merged pull requests:**

- Update to Kafka 3.0.0 [\#129](https://github.com/bakdata/streams-bootstrap/pull/129) ([@philipp94831](https://github.com/philipp94831))

## [1.9.3](https://github.com/bakdata/streams-bootstrap/tree/1.9.3) (2021-12-20)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.9.2...1.9.3)

**Merged pull requests:**

- Update log4j to 2.17.0 [\#128](https://github.com/bakdata/streams-bootstrap/pull/128) ([@philipp94831](https://github.com/philipp94831))

## [1.9.2](https://github.com/bakdata/streams-bootstrap/tree/1.9.2) (2021-12-16)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.9.1...1.9.2)

**Merged pull requests:**

- Update log4j to 2.16.0 [\#127](https://github.com/bakdata/streams-bootstrap/pull/127) ([@philipp94831](https://github.com/philipp94831))

## [1.9.1](https://github.com/bakdata/streams-bootstrap/tree/1.9.1) (2021-12-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.9.0...1.9.1)

**Merged pull requests:**

- Update log4j to 2.15.0 [\#125](https://github.com/bakdata/streams-bootstrap/pull/125) ([@philipp94831](https://github.com/philipp94831))

## [1.9.0](https://github.com/bakdata/streams-bootstrap/tree/1.9.0) (2021-09-14)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.8.0...1.9.0)

**Closed issues:**

- Restart failed clean up jobs rather than creating new pods [\#94](https://github.com/bakdata/streams-bootstrap/issues/94)
- Fix vulnerable dependencies [\#114](https://github.com/bakdata/streams-bootstrap/issues/114)

**Merged pull requests:**

- Upgrade to Kafka 2.8 [\#124](https://github.com/bakdata/streams-bootstrap/pull/124) ([@philipp94831](https://github.com/philipp94831))
- Add workaround for rclone config permissions [\#123](https://github.com/bakdata/streams-bootstrap/pull/123) ([@yannick-roeder](https://github.com/yannick-roeder))
- Fix rclone volume permissions [\#122](https://github.com/bakdata/streams-bootstrap/pull/122) ([@yannick-roeder](https://github.com/yannick-roeder))
- Restart failed cleanup jobs instead of creating new pods [\#120](https://github.com/bakdata/streams-bootstrap/pull/120) ([@yannick-roeder](https://github.com/yannick-roeder))
- Update openbridge/ob\_bulkstash version and allow arguments [\#121](https://github.com/bakdata/streams-bootstrap/pull/121) ([@aqchen](https://github.com/aqchen))
- Downgrad Gradle to 6.7.1 [\#119](https://github.com/bakdata/streams-bootstrap/pull/119) ([@philipp94831](https://github.com/philipp94831))

## [1.8.0](https://github.com/bakdata/streams-bootstrap/tree/1.8.0) (2021-09-03)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.7.1...1.8.0)

**Merged pull requests:**

- Migrate to Log4j 2 [\#115](https://github.com/bakdata/streams-bootstrap/pull/115) ([@philipp94831](https://github.com/philipp94831))
- Suspend rclone cron jobs [\#117](https://github.com/bakdata/streams-bootstrap/pull/117) ([@philipp94831](https://github.com/philipp94831))
- Auto\-scale stateful sets [\#116](https://github.com/bakdata/streams-bootstrap/pull/116) ([@philipp94831](https://github.com/philipp94831))

## [1.7.1](https://github.com/bakdata/streams-bootstrap/tree/1.7.1) (2021-08-20)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.7.0...1.7.1)

**Closed issues:**

- Clean schemas of pseudo topics created when performing a foreign key join [\#112](https://github.com/bakdata/streams-bootstrap/issues/112)
- Extend the Readme [\#99](https://github.com/bakdata/streams-bootstrap/issues/99)

**Merged pull requests:**

- Clean schemas of pseudo topics created when performing a foreign key join [\#113](https://github.com/bakdata/streams-bootstrap/pull/113) ([@philipp94831](https://github.com/philipp94831))
- Support KEDA idleReplicaCount [\#109](https://github.com/bakdata/streams-bootstrap/pull/109) ([@philipp94831](https://github.com/philipp94831))
- producer chart: job/cronjob switch [\#110](https://github.com/bakdata/streams-bootstrap/pull/110) ([@mkcode92](https://github.com/mkcode92))
- Make producer\-app's CronJob schedule optional [\#108](https://github.com/bakdata/streams-bootstrap/pull/108) ([@torbsto](https://github.com/torbsto))
- Fix consumer group annotation [\#107](https://github.com/bakdata/streams-bootstrap/pull/107) ([@philipp94831](https://github.com/philipp94831))
- Fix nil warnings about empty yaml lists in value files [\#106](https://github.com/bakdata/streams-bootstrap/pull/106) ([@yannick-roeder](https://github.com/yannick-roeder))
- Add node affinity to helm chart [\#103](https://github.com/bakdata/streams-bootstrap/pull/103) ([@yannick-roeder](https://github.com/yannick-roeder))
- Reset schema of internal topics when they are deleted  [\#104](https://github.com/bakdata/streams-bootstrap/pull/104) ([@torbsto](https://github.com/torbsto))
- Extend the readme [\#100](https://github.com/bakdata/streams-bootstrap/pull/100) ([@VictorKuenstler](https://github.com/VictorKuenstler))

**Fixed bugs:**

- Add serviceName for StatefulSet [\#102](https://github.com/bakdata/streams-bootstrap/pull/102) ([@disrupted](https://github.com/disrupted))

## [1.7.0](https://github.com/bakdata/streams-bootstrap/tree/1.7.0) (2021-02-18)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.6.0...1.7.0)

**Closed issues:**

- Update to Kafka 2.6/Confluent 6.0 [\#96](https://github.com/bakdata/streams-bootstrap/issues/96)
- Add support for tolerations in streams helm charts [\#86](https://github.com/bakdata/streams-bootstrap/issues/86)

**Merged pull requests:**

- Update Kafka to 2.7 [\#98](https://github.com/bakdata/streams-bootstrap/pull/98) ([@philipp94831](https://github.com/philipp94831))
- Add priority class to helm charts [\#97](https://github.com/bakdata/streams-bootstrap/pull/97) ([@yannick-roeder](https://github.com/yannick-roeder))
- Add consumer group annotation from autoscaling [\#95](https://github.com/bakdata/streams-bootstrap/pull/95) ([@VictorKuenstler](https://github.com/VictorKuenstler))

## [1.6.0](https://github.com/bakdata/streams-bootstrap/tree/1.6.0) (2021-01-25)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.5...1.6.0)

**Merged pull requests:**

- Rename to streams\-bootstrap [\#93](https://github.com/bakdata/streams-bootstrap/pull/93) ([@philipp94831](https://github.com/philipp94831))

## [1.5.5](https://github.com/bakdata/streams-bootstrap/tree/1.5.5) (2021-01-21)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.4...1.5.5)

**Merged pull requests:**

- Expose AdminClient from CleanUpRunner [\#92](https://github.com/bakdata/streams-bootstrap/pull/92) ([@philipp94831](https://github.com/philipp94831))
- Upgrade to helm 3 [\#91](https://github.com/bakdata/streams-bootstrap/pull/91) ([@yannick-roeder](https://github.com/yannick-roeder))
- Reduce helm chart size [\#90](https://github.com/bakdata/streams-bootstrap/pull/90) ([@philipp94831](https://github.com/philipp94831))

## [1.5.4](https://github.com/bakdata/streams-bootstrap/tree/1.5.4) (2020-12-18)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.3...1.5.4)

**Merged pull requests:**

- Add producer application [\#89](https://github.com/bakdata/streams-bootstrap/pull/89) ([@philipp94831](https://github.com/philipp94831))
- Add tolerations to deployment [\#87](https://github.com/bakdata/streams-bootstrap/pull/87) ([@yannick-roeder](https://github.com/yannick-roeder))

## [1.5.3](https://github.com/bakdata/streams-bootstrap/tree/1.5.3) (2020-11-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.2...1.5.3)

**Merged pull requests:**

- Do not delete consumer group if it does not exist [\#85](https://github.com/bakdata/streams-bootstrap/pull/85) ([@philipp94831](https://github.com/philipp94831))

## [1.5.2](https://github.com/bakdata/streams-bootstrap/tree/1.5.2) (2020-11-23)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.1...1.5.2)

**Merged pull requests:**

- Delete consumer groups on delete output [\#84](https://github.com/bakdata/streams-bootstrap/pull/84) ([@philipp94831](https://github.com/philipp94831))
- Fix comment for autoscaling [\#83](https://github.com/bakdata/streams-bootstrap/pull/83) ([@yannick-roeder](https://github.com/yannick-roeder))
- Add scaled object for autoscaling with keda [\#82](https://github.com/bakdata/streams-bootstrap/pull/82) ([@yannick-roeder](https://github.com/yannick-roeder))

## [1.5.1](https://github.com/bakdata/streams-bootstrap/tree/1.5.1) (2020-11-03)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.5.0...1.5.1)

**Merged pull requests:**

- Add extra input topics to streams app [\#81](https://github.com/bakdata/streams-bootstrap/pull/81) ([@philipp94831](https://github.com/philipp94831))
- Remove container suffix from container name [\#73](https://github.com/bakdata/streams-bootstrap/pull/73) ([@yannick-roeder](https://github.com/yannick-roeder))

**Fixed bugs:**

- Container can not be launched if name exceeds 63 characters [\#69](https://github.com/bakdata/streams-bootstrap/issues/69)

## [1.5.0](https://github.com/bakdata/streams-bootstrap/tree/1.5.0) (2020-09-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.6...1.5.0)

**Merged pull requests:**

- Add labels, annotations and configurable name to rclone CronJob [\#80](https://github.com/bakdata/streams-bootstrap/pull/80) ([@philipp94831](https://github.com/philipp94831))
- Update Kafka to 2.5.1 and Confluent to 5.5.1 [\#79](https://github.com/bakdata/streams-bootstrap/pull/79) ([@philipp94831](https://github.com/philipp94831))

## [1.4.6](https://github.com/bakdata/streams-bootstrap/tree/1.4.6) (2020-08-26)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.5...1.4.6)

**Merged pull requests:**

- Close resources after streams client [\#78](https://github.com/bakdata/streams-bootstrap/pull/78) ([@philipp94831](https://github.com/philipp94831))

## [1.4.5](https://github.com/bakdata/streams-bootstrap/tree/1.4.5) (2020-08-06)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.4...1.4.5)

**Closed issues:**

- Use inheritance for streams helm charts [\#71](https://github.com/bakdata/streams-bootstrap/issues/71)

**Merged pull requests:**

- Make helm chart backwards compatible [\#77](https://github.com/bakdata/streams-bootstrap/pull/77) ([@philipp94831](https://github.com/philipp94831))
- Specify additional output topics via default CLI [\#75](https://github.com/bakdata/streams-bootstrap/pull/75) ([@philipp94831](https://github.com/philipp94831))
- Trim rclone job name [\#76](https://github.com/bakdata/streams-bootstrap/pull/76) ([@philipp94831](https://github.com/philipp94831))
- Add annotations to clean up job [\#74](https://github.com/bakdata/streams-bootstrap/pull/74) ([@philipp94831](https://github.com/philipp94831))
- Remove duplicated streams chart [\#72](https://github.com/bakdata/streams-bootstrap/pull/72) ([@philipp94831](https://github.com/philipp94831))
- Fix yaml file for kubernetes \> 1.16 [\#70](https://github.com/bakdata/streams-bootstrap/pull/70) ([@VictorKuenstler](https://github.com/VictorKuenstler))
- Make JAVA\_TOOL\_OPTIONS configurable [\#68](https://github.com/bakdata/streams-bootstrap/pull/68) ([@philipp94831](https://github.com/philipp94831))

## [1.4.4](https://github.com/bakdata/streams-bootstrap/tree/1.4.4) (2020-04-14)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.3...1.4.4)

**Merged pull requests:**

- Close resources on streams creation error [\#67](https://github.com/bakdata/streams-bootstrap/pull/67) ([@philipp94831](https://github.com/philipp94831))
- Only run streams resetter for existing topics [\#66](https://github.com/bakdata/streams-bootstrap/pull/66) ([@philipp94831](https://github.com/philipp94831))
- Use Kubernetes secrets for password parameters [\#64](https://github.com/bakdata/streams-bootstrap/pull/64) ([@yannick-roeder](https://github.com/yannick-roeder))
- Merge streams chart for statefulset and deployment [\#65](https://github.com/bakdata/streams-bootstrap/pull/65) ([@philipp94831](https://github.com/philipp94831))

## [1.4.3](https://github.com/bakdata/streams-bootstrap/tree/1.4.3) (2020-04-02)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.2...1.4.3)

**Implemented enhancements:**

- Override log4j.properties when using provided [\#62](https://github.com/bakdata/streams-bootstrap/pull/62) ([@b-feldmann](https://github.com/b-feldmann))

**Merged pull requests:**

- Revert PicoCli api usage to exit application properly [\#63](https://github.com/bakdata/streams-bootstrap/pull/63) ([@philipp94831](https://github.com/philipp94831))

## [1.4.2](https://github.com/bakdata/streams-bootstrap/tree/1.4.2) (2020-03-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.1...1.4.2)

**Merged pull requests:**

- Run streams resetter for external source topics and intermediate topics [\#61](https://github.com/bakdata/streams-bootstrap/pull/61) ([@philipp94831](https://github.com/philipp94831))
- Add helm chart to deploy streams app as statefulset with static group membership [\#60](https://github.com/bakdata/streams-bootstrap/pull/60) ([@philipp94831](https://github.com/philipp94831))

## [1.4.1](https://github.com/bakdata/streams-bootstrap/tree/1.4.1) (2020-03-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.4.0...1.4.1)

**Merged pull requests:**

- Allow specification of boolean flags without any value [\#59](https://github.com/bakdata/streams-bootstrap/pull/59) ([@philipp94831](https://github.com/philipp94831))
- Add topic client [\#58](https://github.com/bakdata/streams-bootstrap/pull/58) ([@philipp94831](https://github.com/philipp94831))
- Do not exit application [\#57](https://github.com/bakdata/streams-bootstrap/pull/57) ([@philipp94831](https://github.com/philipp94831))

## [1.4.0](https://github.com/bakdata/streams-bootstrap/tree/1.4.0) (2020-03-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.3.4...1.4.0)

**Merged pull requests:**

- Remove DeadLetter [\#56](https://github.com/bakdata/streams-bootstrap/pull/56) ([@philipp94831](https://github.com/philipp94831))
- Move error handlers to com.bakdata.kafka:error\-handling [\#55](https://github.com/bakdata/streams-bootstrap/pull/55) ([@philipp94831](https://github.com/philipp94831))

## [1.3.4](https://github.com/bakdata/streams-bootstrap/tree/1.3.4) (2020-03-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.3.3...1.3.4)

**Merged pull requests:**

- Parse streams config from separate environment variables [\#54](https://github.com/bakdata/streams-bootstrap/pull/54) ([@philipp94831](https://github.com/philipp94831))

## [1.3.3](https://github.com/bakdata/streams-bootstrap/tree/1.3.3) (2020-02-27)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.3.2...1.3.3)

**Merged pull requests:**

- Exit application with proper code [\#53](https://github.com/bakdata/streams-bootstrap/pull/53) ([@philipp94831](https://github.com/philipp94831))

## [1.3.2](https://github.com/bakdata/streams-bootstrap/tree/1.3.2) (2020-02-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.3.1...1.3.2)

**Merged pull requests:**

- Check for errors when running streams resetter [\#52](https://github.com/bakdata/streams-bootstrap/pull/52) ([@philipp94831](https://github.com/philipp94831))

## [1.3.1](https://github.com/bakdata/streams-bootstrap/tree/1.3.1) (2020-01-30)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.3.0...1.3.1)

**Merged pull requests:**

- Add hook for Streams state transitions [\#51](https://github.com/bakdata/streams-bootstrap/pull/51) ([@philipp94831](https://github.com/philipp94831))
- Fix parsing of environment parameters with ENV\_PREFIX in name [\#50](https://github.com/bakdata/streams-bootstrap/pull/50) ([@philipp94831](https://github.com/philipp94831))

## [1.3.0](https://github.com/bakdata/streams-bootstrap/tree/1.3.0) (2020-01-29)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.2.1...1.3.0)

**Merged pull requests:**

- Update Kafka to 2.4.0 [\#49](https://github.com/bakdata/streams-bootstrap/pull/49) ([@philipp94831](https://github.com/philipp94831))

## [1.2.1](https://github.com/bakdata/streams-bootstrap/tree/1.2.1) (2020-01-10)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.2.0...1.2.1)

**Merged pull requests:**

- Configure StreamsResetter with application properties [\#48](https://github.com/bakdata/streams-bootstrap/pull/48) ([@torbsto](https://github.com/torbsto))
- Support schema registry authentication [\#47](https://github.com/bakdata/streams-bootstrap/pull/47) ([@philipp94831](https://github.com/philipp94831))

## [1.2.0](https://github.com/bakdata/streams-bootstrap/tree/1.2.0) (2020-01-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.13...1.2.0)

**Implemented enhancements:**

- Call close after application clean up [\#46](https://github.com/bakdata/streams-bootstrap/pull/46) ([@philipp94831](https://github.com/philipp94831))
- Apply overridden Kafka config before CLI passed configuration [\#44](https://github.com/bakdata/streams-bootstrap/issues/44)

**Merged pull requests:**

- Prioritize Kafka Streams Config passed via CLI over overridden properties [\#45](https://github.com/bakdata/streams-bootstrap/pull/45) ([@philipp94831](https://github.com/philipp94831))
- Add custom annotations [\#43](https://github.com/bakdata/streams-bootstrap/pull/43) ([@SvenLehmann](https://github.com/SvenLehmann))
- Improve values.yaml structure [\#42](https://github.com/bakdata/streams-bootstrap/pull/42) ([@SvenLehmann](https://github.com/SvenLehmann))
- Allow custom labels for jobs and deployments [\#41](https://github.com/bakdata/streams-bootstrap/pull/41) ([@SvenLehmann](https://github.com/SvenLehmann))
- Reset internal topics [\#40](https://github.com/bakdata/streams-bootstrap/pull/40) ([@torbsto](https://github.com/torbsto))

## [1.1.13](https://github.com/bakdata/streams-bootstrap/tree/1.1.13) (2019-11-15)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.12...1.1.13)

**Merged pull requests:**

- Classify all Kafka errors as recoverable bar some exceptions [\#38](https://github.com/bakdata/streams-bootstrap/pull/38) ([@philipp94831](https://github.com/philipp94831))
- Make PodAffinity rule configurable [\#37](https://github.com/bakdata/streams-bootstrap/pull/37) ([@SvenLehmann](https://github.com/SvenLehmann))
- Add flat value transformers for error handling [\#36](https://github.com/bakdata/streams-bootstrap/pull/36) ([@philipp94831](https://github.com/philipp94831))

## [1.1.12](https://github.com/bakdata/streams-bootstrap/tree/1.1.12) (2019-11-07)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.11...1.1.12)

**Merged pull requests:**

- Fix schema registry clean up [\#35](https://github.com/bakdata/streams-bootstrap/pull/35) ([@philipp94831](https://github.com/philipp94831))

## [1.1.11](https://github.com/bakdata/streams-bootstrap/tree/1.1.11) (2019-11-05)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.10...1.1.11)

**Merged pull requests:**

- Improve code quality [\#34](https://github.com/bakdata/streams-bootstrap/pull/34) ([@philipp94831](https://github.com/philipp94831))
- Add common error handlers [\#33](https://github.com/bakdata/streams-bootstrap/pull/33) ([@philipp94831](https://github.com/philipp94831))

## [1.1.10](https://github.com/bakdata/streams-bootstrap/tree/1.1.10) (2019-10-31)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.9...1.1.10)

**Merged pull requests:**

-  Delete schemas during clean up  [\#32](https://github.com/bakdata/streams-bootstrap/pull/32) ([@torbsto](https://github.com/torbsto))
- Add a helm chart for running Streams App as a job [\#31](https://github.com/bakdata/streams-bootstrap/pull/31) ([@SvenLehmann](https://github.com/SvenLehmann))

## [1.1.9](https://github.com/bakdata/streams-bootstrap/tree/1.1.9) (2019-10-25)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.8...1.1.9)

**Merged pull requests:**

- Delete error topic if requested [\#30](https://github.com/bakdata/streams-bootstrap/pull/30) ([@SvenLehmann](https://github.com/SvenLehmann))

## [1.1.8](https://github.com/bakdata/streams-bootstrap/tree/1.1.8) (2019-10-17)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.7...1.1.8)

**Merged pull requests:**

- Expose delete topic method to subclasses [\#29](https://github.com/bakdata/streams-bootstrap/pull/29) ([@philipp94831](https://github.com/philipp94831))

## [1.1.7](https://github.com/bakdata/streams-bootstrap/tree/1.1.7) (2019-10-16)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.6...1.1.7)

**Merged pull requests:**

- Change reprocessing to clean up [\#26](https://github.com/bakdata/streams-bootstrap/pull/26) ([@torbsto](https://github.com/torbsto))

## [1.1.6](https://github.com/bakdata/streams-bootstrap/tree/1.1.6) (2019-10-11)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.5...1.1.6)

**Merged pull requests:**

- Add utility method for single input topic [\#28](https://github.com/bakdata/streams-bootstrap/pull/28) ([@torbsto](https://github.com/torbsto))

## [1.1.5](https://github.com/bakdata/streams-bootstrap/tree/1.1.5) (2019-10-08)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.4...1.1.5)

**Merged pull requests:**

- Allow multiple input topics and add an error topic [\#27](https://github.com/bakdata/streams-bootstrap/pull/27) ([@torbsto](https://github.com/torbsto))

## [1.1.4](https://github.com/bakdata/streams-bootstrap/tree/1.1.4) (2019-09-11)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.3...1.1.4)

**Closed issues:**

- External stream configuration parameter cannot handle primitive data types other than String [\#23](https://github.com/bakdata/streams-bootstrap/issues/23)

**Merged pull requests:**

- Add hook for registering an uncaught exception handler [\#25](https://github.com/bakdata/streams-bootstrap/pull/25) ([@philipp94831](https://github.com/philipp94831))
- Add rclone chart [\#24](https://github.com/bakdata/streams-bootstrap/pull/24) ([@lawben](https://github.com/lawben))
- Allow kafka streams configuration with external parameter [\#22](https://github.com/bakdata/streams-bootstrap/pull/22) ([@fapaul](https://github.com/fapaul))
- Add JMX prometheus [\#21](https://github.com/bakdata/streams-bootstrap/pull/21) ([@lawben](https://github.com/lawben))

## [1.1.3](https://github.com/bakdata/streams-bootstrap/tree/1.1.3) (2019-08-01)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.2...1.1.3)

**Implemented enhancements:**

- Add CLI parameter to allow reprocessing of data [\#14](https://github.com/bakdata/streams-bootstrap/pull/14) ([@SvenLehmann](https://github.com/SvenLehmann))

**Merged pull requests:**

- Release new version of kafka\-streams chart [\#20](https://github.com/bakdata/streams-bootstrap/pull/20) ([@fapaul](https://github.com/fapaul))
- Change clean up visibility [\#19](https://github.com/bakdata/streams-bootstrap/pull/19) ([@fapaul](https://github.com/fapaul))
- Change reset visibility [\#18](https://github.com/bakdata/streams-bootstrap/pull/18) ([@fapaul](https://github.com/fapaul))
- Initialize topolgy stream before cleanup [\#17](https://github.com/bakdata/streams-bootstrap/pull/17) ([@fapaul](https://github.com/fapaul))
-  Add cleanup possibility on processor startup [\#15](https://github.com/bakdata/streams-bootstrap/pull/15) ([@fapaul](https://github.com/fapaul))
- Fix duplicate in app name [\#16](https://github.com/bakdata/streams-bootstrap/pull/16) ([@lawben](https://github.com/lawben))
- Change log level on debug for child app in different package [\#13](https://github.com/bakdata/streams-bootstrap/pull/13) ([@fapaul](https://github.com/fapaul))
- Add unique AppID method [\#12](https://github.com/bakdata/streams-bootstrap/pull/12) ([@lawben](https://github.com/lawben))
- Log initial configuration on debug [\#11](https://github.com/bakdata/streams-bootstrap/pull/11) ([@fapaul](https://github.com/fapaul))

## [1.1.2](https://github.com/bakdata/streams-bootstrap/tree/1.1.2) (2019-06-27)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.1...1.1.2)

**Merged pull requests:**

- Add support for AWS roles and pod resources [\#10](https://github.com/bakdata/streams-bootstrap/pull/10) ([@SvenLehmann](https://github.com/SvenLehmann))
- Update readme with latest release version number [\#9](https://github.com/bakdata/streams-bootstrap/pull/9) ([@fapaul](https://github.com/fapaul))

## [1.1.1](https://github.com/bakdata/streams-bootstrap/tree/1.1.1) (2019-05-24)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.1.0...1.1.1)

**Merged pull requests:**

- Add log4j binding for sl4j [\#8](https://github.com/bakdata/streams-bootstrap/pull/8) ([@fapaul](https://github.com/fapaul))
- Set arity for boolean options to 1 to match environment key value pairs [\#7](https://github.com/bakdata/streams-bootstrap/pull/7) ([@fapaul](https://github.com/fapaul))
- Comply default stream application name schema with kube dns [\#6](https://github.com/bakdata/streams-bootstrap/pull/6) ([@fapaul](https://github.com/fapaul))

## [1.1.0](https://github.com/bakdata/streams-bootstrap/tree/1.1.0) (2019-05-14)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.0.1...1.1.0)

**Merged pull requests:**

- Remove default custom environment variables ingestion from values.yaml [\#5](https://github.com/bakdata/streams-bootstrap/pull/5) ([@lawben](https://github.com/lawben))
- Add default log properties [\#4](https://github.com/bakdata/streams-bootstrap/pull/4) ([@lawben](https://github.com/lawben))
- Bakdata Kafka Streams Helm Repository [\#3](https://github.com/bakdata/streams-bootstrap/pull/3) ([@fapaul](https://github.com/fapaul))

## [1.0.1](https://github.com/bakdata/streams-bootstrap/tree/1.0.1) (2019-03-27)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/1.0.0...1.0.1)


## [1.0.0](https://github.com/bakdata/streams-bootstrap/tree/1.0.0) (2019-03-13)
[View commits](https://github.com/bakdata/streams-bootstrap/compare/a9757f7ec96ccc43c91d668d00314d9106b9b9c6...1.0.0)

**Closed issues:**

- Extract common Kafka Streams parts [\#1](https://github.com/bakdata/streams-bootstrap/issues/1)

**Merged pull requests:**

- Kafka streams application [\#2](https://github.com/bakdata/streams-bootstrap/pull/2) ([@fapaul](https://github.com/fapaul))
