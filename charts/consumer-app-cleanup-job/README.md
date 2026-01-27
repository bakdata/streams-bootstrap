# consumer-app cleanup helm chart

This chart can be used to deploy a cleanup job for your Kafka Consumer app developed using streams-bootstrap.
Make sure to destroy the corresponding consumer deployment before running the cleanup job.

## Configuration

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart.

All relevant configurations of
the [consumer app](https://github.com/bakdata/streams-bootstrap/tree/master/charts/consumer-app) are available as well.
Therefore, you can just reuse your `values.yaml` file.

Additionally, the following parameters can be configured:

| Parameter       | Description                                                                                                                                        | Default     |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `restartPolicy` | [Restart policy](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy) to use for the job.                             | `OnFailure` |
| `deleteOutput`  | Whether the consumer group should be deleted.                                                  | `false`     |
| `backoffLimit`  | The number of times to restart an unsuccessful job. See https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy. | `6`         |
