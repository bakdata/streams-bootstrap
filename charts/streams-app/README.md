# streams-app helm chart

This chart can be used to deploy a Kafka Streams app developed using streams-bootstrap.

## Configuration

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart.

### Deployment

| Parameter                       | Description                                                                                                                                                                                                                                                | Default                                    |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `nameOverride`                  | The name of the Kubernetes deployment.                                                                                                                                                                                                                     | `bakdata-streams-app`                      |
| `replicaCount`                  | The number of Kafka Streams replicas.                                                                                                                                                                                                                      | `1`                                        |
| `resources`                     | See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/                                                                                                                                                                         | see [values.yaml](values.yaml) for details |
| `annotations`                   | Map of custom annotations to attach to the deployment.                                                                                                                                                                                                     | `{}`                                       |
| `labels`                        | Map of custom labels to attach to the deployment.                                                                                                                                                                                                          | `{}`                                       |
| `tolerations`                   | Array containing taint references. When defined, pods can run on nodes, which would otherwise deny scheduling. Further information can be found in the [Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/) | `{}`                                       |
| `statefulSet`                   | Whether to use a [Statefulset](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) instead of a [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) to deploy the streams app.                         | `false`                                    |
| `priorityClassName`             | [Priority class name](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/) for the pod.                                                                                                                                       |                                            |
| `affinity`                      | Map to configure [pod affinities](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity).                                                                                                                          | `{}`                                       |
| `ports.containerPort`           | Number of the port to expose.                                                                                                                                                                                                                              |                                            |
| `ports.name`                    | Services can reference port by name (optional).                                                                                                                                                                                                            |                                            |
| `ports.schema`                  | Protocol for port. Must be UDP, TCP, or SCTP (optional).                                                                                                                                                                                                   |                                            |
| `ports.servicePort`             | Number of the port of the service (optional). See [service definition](#service)                                                                                                                                                                           |                                            |
| `livenessProbe`                 | Probe v1 definition for streams-app: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#probe-v1-core                                                                                                                                    | `{}`                                       |
| `readinessProbe`                | Probe v1 definition for streams-app: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#probe-v1-core                                                                                                                                    | `{}`                                       |
| `podAnnotations`                | Map of custom annotations to attach to the pod spec.                                                                                                                                                                                                       | `{}`                                       |
| `podLabels`                     | Map of custom labels to attach to the pod spec.                                                                                                                                                                                                            | `{}`                                       |
| `terminationGracePeriodSeconds` | Delay for graceful application shutdown in seconds: https://pracucci.com/graceful-shutdown-of-kubernetes-pods.html                                                                                                                                         | `300`                                      |

### Storage

| Parameter                  | Description                                                                                                                       | Default |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `persistence.enabled`      | Use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) to store the state of the streams app. | `false` |
| `persistence.size`         | Size of the persistent volume.                                                                                                    | `1Gi`   |
| `persistence.storageClass` | Storage class to use for the persistent volume.                                                                                   |         |

### Image

| Parameter          | Description                                | Default      |
| ------------------ | ------------------------------------------ | ------------ |
| `image`            | Docker image of the Kafka Streams app.     | `streamsApp` |
| `imageTag`         | Docker image tag of the Kafka Streams app. | `latest`     |
| `imagePullPolicy`  | Docker image pull policy.                  | `Always`     |
| `imagePullSecrets` | Secrets to be used for private registries. |              |

### Kafka

| Parameter                    | Description                                                                                                                                                                      | Default |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `kafka.bootstrapServers`     | Comma separated list of Kafka bootstrap servers to connect to.                                                                                                                   |         |
| `kafka.schemaRegistryUrl`    | URL of Schema Registry to connect to.                                                                                                                                            | `null`  |
| `kafka.staticMembership`     | Whether to use [Kafka Static Group Membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances). | `false` |
| `kafka.config`               | Configurations for your [Kafka Streams app](https://kafka.apache.org/documentation/#streamsconfigs).                                                                             | `{}`    |
| `kafka.inputTopics`          | List of input topics for your streams application.                                                                                                                               | `[]`    |
| `kafka.labeledInputTopics`   | Map of additional labeled input topics if you need to specify multiple topics with different message types.                                                                      | `{}`    |
| `kafka.inputPattern`         | Input pattern of topics for your streams application.                                                                                                                            |         |
| `kafka.labeledInputPatterns` | Map of additional labeled input patterns if you need to specify multiple topics with different message types.                                                                    | `{}`    |
| `kafka.outputTopic`          | Output topic for your streams application.                                                                                                                                       |         |
| `kafka.labeledOutputTopics`  | Map of additional labeled output topics if you need to specify multiple topics with different message types.                                                                     | `{}`    |
| `kafka.errorTopic`           | Error topic for your streams application.                                                                                                                                        |         |
| `kafka.applicationId`        | Unique application ID for Kafka Streams. Required for auto-scaling                                                                                                               |         |

### Other

| Parameter                | Description                                                                                                                                                                                                                                                                                                                   | Default |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `configurationEnvPrefix` | Prefix for environment variables to use that should be parsed as command line arguments.                                                                                                                                                                                                                                      | `APP`   |
| `commandLine`            | Map of command line arguments passed to the streams app.                                                                                                                                                                                                                                                                      | `{}`    |
| `env`                    | Custom environment variables                                                                                                                                                                                                                                                                                                  | `{}`    |
| `secrets`                | Custom secret environment variables. Prefix with `configurationEnvPrefix` in order to pass secrets to command line or prefix with `KAFKA_` to pass secrets to Kafka Streams configuration. E.g., `APP_MY_PARAM` would be passed as `--my-param` and `KAFKA_MAX_POLL_TIMEOUT_MS` would be translated to `max.poll.timeout.ms`. | `{}`    |
| `secretRefs`             | Inject existing secrets as environment variables. Map key is used as environment variable name. Value consists of secret `name` and `key`.                                                                                                                                                                                    | `{}`    |
| `secretFilesRefs`        | Mount existing secrets as volumes                                                                                                                                                                                                                                                                                             | `[]`    |
| `files`                  | Map of files to mount for the app. File will be mounted as `$value.mountPath/$key`. `$value.content` denotes file content (recommended to be used with `--set-file`).                                                                                                                                                         | `{}`    |

### JMX Configuration

| Parameter     | Description                                                             | Default     |
|---------------|-------------------------------------------------------------------------|-------------|
| `jmx.enabled` | Whether or not to open JMX port for remote access (e.g., for debugging) | `false`     |
| `jmx.port`    | The JMX port which JMX style metrics are exposed.                       | `5555`      |
| `jmx.host`    | The host to use for JMX remote access.                                  | `localhost` |

### Prometheus JMX Exporter Configuration

| Parameter                        | Description                                                                                                    | Default                                    |
|----------------------------------|----------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `prometheus.jmx.enabled`         | Whether or not to install Prometheus JMX Exporter as a sidecar container and expose JMX metrics to Prometheus. | `false`                                    |
| `prometheus.jmx.image`           | Docker Image for Prometheus JMX Exporter container.                                                            | `bitnami/jmx-exporter`                     |
| `prometheus.jmx.imageTag`        | Docker Image Tag for Prometheus JMX Exporter container.                                                        | `1.1.0`                                    |
| `prometheus.jmx.imagePullPolicy` | Docker Image Pull Policy for Prometheus JMX Exporter container.                                                | `Always`                                   |
| `prometheus.jmx.port`            | JMX Exporter Port which exposes metrics in Prometheus format for scraping.                                     | `5556`                                     |
| `prometheus.jmx.metricRules`     | List of JMX metric rules.                                                                                      | `[pattern: ".*"]`                          |
| `prometheus.jmx.resources`       | JMX Exporter resources configuration.                                                                          | see [values.yaml](values.yaml) for details |

Prometheus can scrape your metrics by deploying
a [PodMonitor](https://github.com/bakdata/streams-bootstrap/blob/master/monitoring/pod_monitor.yaml) or adding pod
annotations

```yaml
podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: "/metrics"
  prometheus.io/port: 5556 # needs to match prometheus.jmx.port
```

### Auto-Scaling

| Parameter                        | Description                                                                                                        | Default    |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ---------- |
| `autoscaling.enabled`            | Whether to enable auto-scaling using [KEDA](https://keda.sh/docs/latest/scalers/apache-kafka/).                    | `false`    |
| `autoscaling.lagThreshold`       | Average target value to trigger scaling actions.                                                                   |            |
| `autoscaling.pollingInterval`    | https://keda.sh/docs/2.10/concepts/scaling-deployments/#pollinginterval                                            | `30`       |
| `autoscaling.cooldownPeriod`     | https://keda.sh/docs/2.10/concepts/scaling-deployments/#cooldownperiod                                             | `300`      |
| `autoscaling.offsetResetPolicy`  | The offset reset policy for the consumer if the the consumer group is not yet subscribed to a partition.           | `earliest` |
| `autoscaling.minReplicas`        | https://keda.sh/docs/2.10/concepts/scaling-deployments/#minreplicacount                                            | `0`        |
| `autoscaling.maxReplicas`        | https://keda.sh/docs/2.10/concepts/scaling-deployments/#maxreplicacount                                            | `1`        |
| `autoscaling.idleReplicas`       | https://keda.sh/docs/2.10/concepts/scaling-deployments/#idlereplicacount                                           |            |
| `autoscaling.internalTopics`     | List of auto-generated Kafka Streams topics used by the streams app. Consumer group prefix is added automatically. | `[]`       |
| `autoscaling.topics`             | List of topics used by the streams app.                                                                            | `[]`       |
| `autoscaling.additionalTriggers` | List of additional KEDA triggers, see https://keda.sh/docs/latest/scalers/                                         | `[]`       |

### JVM

| Parameter                      | Description                                                                                                                                  | Default |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `javaOptions.maxRAMPercentage` | https://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#:~:text=is%20set%20ergonomically.-,%2DXX%3AMaxRAMPercentage,-%3Dpercent | `true`  |
| `javaOptions.others`           | List of Java VM options passed to the streams app.                                                                                           | `[]`    |

### Service

| Parameter         | Description                                                                                                                                                  | Default     |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `service.enabled` | Whether to create a service. This requires the definition of at least one `ports.servicePort`. This also configures `application.server` for the streams app | `false`     |
| `service.labels`  | Additional service labels.                                                                                                                                   | `{}`        |
| `service.type`    | Service type.                                                                                                                                                | `ClusterIP` |
