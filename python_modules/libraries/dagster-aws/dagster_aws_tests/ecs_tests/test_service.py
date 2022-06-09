from dagster_aws.ecs.client import DagsterEcsClient
from dagster_aws.ecs.service import DagsterEcsService


def test_arn():
    short_arn = "arn:aws:ecs:us-east-1:1234567890:service/service-name"
    long_arn = "arn:aws:ecs:us-east-1:1234567890:service/cluster-name/service-name"

    client = DagsterEcsClient(
        cluster_name="cluster-name",
        service_discovery_namespace_id="fake-namespace",
        log_group="fake-log-group",
        execution_role_arn="fake-role",
    )

    service = DagsterEcsService(client=client, arn=short_arn)
    assert service.arn == long_arn

    service = DagsterEcsService(client=client, arn=long_arn)
    assert service.arn == long_arn
