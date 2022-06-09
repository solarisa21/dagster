# pylint: disable=protected-access
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
import json
import re

import boto3
import pytest
from botocore.stub import ANY, Stubber
from dagster_aws.ecs import DagsterEcsClient, DagsterEcsService, EcsServiceError
from dagster_aws_tests.ecs_tests.stubbed_ecs import StubbedEcs


@pytest.fixture
def client(cluster_name="test"):
    client = DagsterEcsClient(
        cluster_name=cluster_name,
        service_discovery_namespace_id="fake-namespace",
        log_group="fake-log-group",
        execution_role_arn="fake-role",
        wait_for_service=False,
    )

    def _mock_assign_public_ip(*args, **kwags):
        return "ENABLED"

    client._assign_public_ip = _mock_assign_public_ip

    yield client


@pytest.fixture
def stubbed_ecs():
    return StubbedEcs(boto3.client("ecs"))


@pytest.fixture
def stubbed_client(stubbed_ecs, cluster_name="test"):
    client = DagsterEcsClient(
        cluster_name=cluster_name,
        service_discovery_namespace_id="fake-namespace",
        log_group="fake-log-group",
        execution_role_arn="fake-role",
        ecs_client=stubbed_ecs,
        wait_for_service=False,
    )

    def _mock_assign_public_ip(*args, **kwags):
        return "ENABLED"

    client._assign_public_ip = _mock_assign_public_ip

    yield client


@pytest.fixture
def stubber(client):
    with Stubber(client.ecs) as stubber:
        yield stubber


@pytest.mark.parametrize("cluster_name", ["test", "arn:aws:ecs:region:012345678910:cluster/test"])
def test_cluster_name(client, cluster_name):
    assert client.cluster_name == "test"


def test_taggable(client, stubber):
    stubber.add_response(
        method="list_account_settings",
        service_response={
            "settings": [
                {"name": "serviceLongArnFormat", "value": "enabled"},
            ]
        },
    )
    assert client.taggable

    stubber.add_response(
        method="list_account_settings",
        service_response={
            "settings": [
                {"name": "serviceLongArnFormat", "value": "disabled"},
            ]
        },
    )
    assert not client.taggable


def test_create_service_tags(client, stubber):
    arn = "arn:aws:ecs:us-east-1:1234567890:service/cluster-name/service-name"
    tags = {"foo": "bar"}

    params_without_tags = [
        "cluster",
        "desiredCount",
        "launchType",
        "networkConfiguration",
        "serviceName",
        "serviceRegistries",
        "taskDefinition",
    ]

    # When the new ARN format is disabled
    stubber.add_response(
        method="list_account_settings",
        service_response={
            "settings": [
                {"name": "serviceLongArnFormat", "value": "disabled"},
            ]
        },
    )
    # Expect that tags don't get added
    expected_params_without_tags = dict.fromkeys(params_without_tags, ANY)
    stubber.add_response(
        method="create_service",
        service_response={"service": {"serviceArn": arn}},
        expected_params=expected_params_without_tags,
    )

    client._create_service(
        service_name="fake",
        service_registry_arn="fake",
        task_definition_arn="fake",
        tags=tags,
    )

    # When the new ARN format is enabled
    stubber.add_response(
        method="list_account_settings",
        service_response={
            "settings": [
                {"name": "serviceLongArnFormat", "value": "enabled"},
            ]
        },
    )
    # Expect that tags get added
    expected_params_with_tags = dict(
        expected_params_without_tags, tags=[{"key": "foo", "value": "bar"}]
    )
    stubber.add_response(
        method="create_service",
        service_response={"service": {"serviceArn": arn}},
        expected_params=expected_params_with_tags,
    )

    client._create_service(
        service_name="fake",
        service_registry_arn="fake",
        task_definition_arn="fake",
        tags=tags,
    )


def test_ecs_service_error():
    with pytest.raises(EcsServiceError) as ex:
        raise EcsServiceError("foo", "bar")

    ex.match("ECS service failed because task foo failed:\nbar")


def test_secrets(stubbed_client, stubbed_ecs):
    assert len(stubbed_ecs.list_task_definitions()["taskDefinitionArns"]) == 0

    secret_dict = {
        "secret_name1": "arn:aws:secretsmanager:us-west-2:111122223333:secret:aes128-1a2b3c",
        "secret_name2": "arn:aws:secretsmanager:us-west-2:111122223333:secret:aes192-4D5e6F",
    }

    stubbed_client.create_service(
        name="fake_service",
        image="fake_image",
        command=["fake_command"],
        register_service_discovery=False,
        secrets=secret_dict,
    )

    task_arns = stubbed_ecs.list_task_definitions()["taskDefinitionArns"]
    assert len(task_arns) == 1
    task_def = stubbed_ecs.describe_task_definition(taskDefinition=task_arns[0])

    assert task_def["taskDefinition"]["containerDefinitions"][0]["secrets"] == [
        {"name": secret_name, "valueFrom": secret_arn}
        for secret_name, secret_arn in secret_dict.items()
    ]


def test_wait_for_service_failure(client, stubber):
    client.wait_for_service = True
    service = DagsterEcsService(arn="arn:aws:ecs:region:012345678910:cluster/boom", client=client)

    failures = [
        {
            "arn": service.arn,
            "detail": "detail",
            "reason": "reason",
        },
    ]
    stubber.add_response(
        method="describe_services",
        service_response={
            "services": [],
            "failures": failures,
        },
    )

    with pytest.raises(
        Exception,
        match=re.escape(f"ECS DescribeTasks API returned failures: {json.dumps(failures)}"),
    ):
        client._wait_for_service(service)

    failures = [
        {
            "arn": service.arn,
            "detail": "detail1",
            "reason": "reason1",
        },
        {
            "arn": service.arn,
            "detail": "detail2",
            "reason": "reason2",
        },
    ]

    stubber.add_response(
        method="describe_services",
        service_response={
            "services": [],
            "failures": failures,
        },
    )
    with pytest.raises(
        Exception,
        match=re.escape(f"ECS DescribeTasks API returned failures: {json.dumps(failures)}"),
    ) as ex:
        client._wait_for_service(service)
    assert ex.match("detail1")
    assert ex.match("detail2")

    stubber.add_response(
        method="describe_services",
        service_response={
            "services": [],
            "failures": [],
        },
    )

    with pytest.raises(
        Exception,
        match=re.escape(
            f"ECS DescribeTasks API returned an empty response for service {service.arn}."
        ),
    ):
        client._wait_for_service(service)
