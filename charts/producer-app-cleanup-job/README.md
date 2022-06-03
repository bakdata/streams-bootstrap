# producer-app cleanup helm chart

This chart can be used to deploy a cleanup job for your Kafka producer app developed using streams-bootstrap.
Make sure to destroy the corresponding producer deployment before running the cleanup job.

## Configuration

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart.

All relevant configurations of
the [producer app](https://github.com/bakdata/streams-bootstrap/tree/master/charts/producer-app) are available as well.
Therefore, you can just reuse your `values.yaml` file.
