# producer-app helm chart

This chart can be used to deploy a Kafka producer app developed using streams-bootstrap.

## Configuration

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart.

### Job

| Parameter                    | Description                                                                                                                                                                                                                                                                | Default                                    |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `nameOverride`               | The name of the Kubernetes deployment.                                                                                                                                                                                                                                     | `bakdata-producer-app`                     |
| `resources`                  | See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/                                                                                                                                                                                         | see [values.yaml](values.yaml) for details |
| `annotations`                | Map of custom annotations to attach to the deployment.                                                                                                                                                                                                                     | `{}`                                       |
| `labels`                     | Map of custom labels to attach to the deployment.                                                                                                                                                                                                                          | `{}`                                       |
| `tolerations`                | Array containing taint references. When defined, pods can run on nodes, which would otherwise deny scheduling. Further information can be found in the [Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)                 | `{}`                                       |
| `affinity`                   | Map to configure [pod affinities](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity).                                                                                                                                                 | `{}`                                       |
| `deployment`                 | Deploy the producer as a Kubernetes Deployment (thereby ignoring Job-related configurations)                                                                                                                                                                               | false                                      |
| `restartPolicy`              | [Restart policy](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy) to use for the job.                                                                                                                                                     | `OnFailure`                                |
| `schedule`                   | Cron expression to denote a schedule this producer app should be run on. It will then be deployed as a [CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/) instead of a [Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/). |                                            |
| `suspend`                    | Whether to suspend the execution of the cron job.                                                                                                                                                                                                                          | `false`                                    |
| `successfulJobsHistoryLimit` | The number of successful jobs to retain. See https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#jobs-history-limits.                                                                                                                                     | `1`                                        |
| `failedJobsHistoryLimit`     | The number of unsuccessful jobs to retain. See https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#jobs-history-limits.                                                                                                                                   | `1`                                        |
| `backoffLimit`               | The number of times to restart an unsuccessful job. See https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy.                                                                                                                         | `6`                                        |
| `ttlSecondsAfterFinished`    | See https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/#ttl-after-finished-controller.                                                                                                                                                             | `100`                                      |
| `ports.containerPort`        | Number of the port to expose.                                                                                                                                                                                                                                              |                                            |
| `ports.name`                 | Services can reference port by name (optional).                                                                                                                                                                                                                            |                                            |
| `ports.schema`               | Protocol for port. Must be UDP, TCP, or SCTP (optional).                                                                                                                                                                                                                   |                                            |
| `ports.servicePort`          | Number of the port of the service (optional). See [service definition](#service)                                                                                                                                                                                           |                                            |
| `livenessProbe`              | Probe v1 definition for producer-app: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#probe-v1-core                                                                                                                                                   | `{}`                                       |
| `readinessProbe`             | Probe v1 definition for producer-app: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#probe-v1-core                                                                                                                                                   | `{}`                                       |
| `podAnnotations`             | Map of custom annotations to attach to the pod spec.                                                                                                                                                                                                                       | `{}`                                       |
| `podLabels`                  | Map of custom labels to attach to the pod spec.                                                                                                                                                                                                                            | `{}`                                       |

### Image

| Parameter          | Description                                 | Default       |
| ------------------ | ------------------------------------------- | ------------- |
| `image`            | Docker image of the Kafka producer app.     | `producerApp` |
| `imageTag`         | Docker image tag of the Kafka producer app. | `latest`      |
| `imagePullPolicy`  | Docker image pull policy.                   | `Always`      |
| `imagePullSecrets` | Secrets to be used for private registries.  |               |

### Streams

| Parameter                   | Description                                                                                                | Default |
|-----------------------------|------------------------------------------------------------------------------------------------------------|---------|
| `streams.brokers`           | Comma separated list of Kafka brokers to connect to.                                                       |         |
| `streams.schemaRegistryUrl` | URL of Schema Registry to connect to.                                                                      | `null`  |
| `streams.config`            | Configurations for your [Kafka producer app](https://kafka.apache.org/documentation/#producerconfigs).     | `{}`    |
| `streams.outputTopic`       | Output topic for your producer application.                                                                |         |
| `streams.namedOutputTopics` | Map of additional named output topics if you need to specify multiple topics with different message types. | `{}`    |

### Other

| Parameter                | Description                                                                                                                                                                                                                                                                                                                       | Default |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `configurationEnvPrefix` | Prefix for environment variables to use that should be parsed as command line arguments.                                                                                                                                                                                                                                          | `APP`   |
| `commandLine`            | Map of command line arguments passed to the producer app.                                                                                                                                                                                                                                                                         | `{}`    |
| `debug`                  | Configure logging to debug                                                                                                                                                                                                                                                                                                        | `false` |
| `env`                    | Custom environment variables                                                                                                                                                                                                                                                                                                      | `{}`    |
| `secrets`                | Custom secret environment variables. Prefix with `configurationEnvPrefix` in order to pass secrets to command line or prefix with `STREAMS_` to pass secrets to Kafka Streams configuration. E.g., `APP_MY_PARAM` would be passed as `--my-param` and `STREAMS_MAX_POLL_TIMEOUT_MS` would be translated to `max.poll.timeout.ms`. | `{}`    |
| `secretRefs`             | Inject existing secrets as environment variables. Map key is used as environment variable name. Value consists of secret `name` and `key`.                                                                                                                                                                                        | `{}`    |
| `secretFilesRefs`        | Mount existing secrets as volumes                                                                                                                                                                                                                                                                                                 | `[]`    |
| `files`                  | Map of files to mount for the app. File will be mounted as `$value.mountPath/$key`. `$value.content` denotes file content (recommended to be used with `--set-file`).                                                                                                                                                             | `{}`    |

### JVM

| Parameter                      | Description                                                                                                                                  | Default |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `javaOptions.maxRAMPercentage` | https://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#:~:text=is%20set%20ergonomically.-,%2DXX%3AMaxRAMPercentage,-%3Dpercent | `true`  |
| `javaOptions.others`           | List of Java VM options passed to the producer app.                                                                                          | `[]`    |

### Service

| Parameter         | Description                                                                                    | Default     |
| ----------------- | ---------------------------------------------------------------------------------------------- | ----------- |
| `service.enabled` | Whether to create a service. This requires the definition of at least one `ports.servicePort`. | `false`     |
| `service.labels`  | Additional service labels.                                                                     | `{}`        |
| `service.type`    | Service type.                                                                                  | `ClusterIP` |
