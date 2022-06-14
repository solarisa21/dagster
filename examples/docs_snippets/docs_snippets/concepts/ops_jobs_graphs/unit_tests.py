# isort: skip_file
# pylint: disable=reimported,unnecessary-ellipsis,unused-variable,unused-argument
from dagster import (
    AssetMaterialization,
    DagsterEventType,
    ExpectationResult,
    ExecuteInProcessResult,
    In,
    Output,
    Out,
    op,
    io_manager,
    IOManager,
    graph,
)


@op(ins={"num": In(dagster_type=int, default_value=1)})
def add_one(num: int) -> int:
    return num + 1


@op(ins={"num": In(dagster_type=int, default_value=1)})
def add_two(num: int) -> int:
    return num + 2


@op
def subtract(left: int, right: int) -> int:
    return left - right


@graph
def do_math():
    subtract(add_one(), add_two())


do_math_job = do_math.to_job()


@op(ins={"input_num": In(dagster_type=int)}, out={"a_num": Out(dagster_type=int)})
def emit_events_op(input_num):
    a_num = input_num + 1
    yield ExpectationResult(
        success=a_num > 0, label="positive", description="A num must be positive"
    )
    yield AssetMaterialization(
        asset_key="persisted_string",
        description="Let us pretend we persisted the string somewhere",
    )
    yield Output(value=a_num, output_name="a_num")


@graph
def emit_events():
    emit_events_op()


emit_events_job = emit_events.to_job()


# start_test_job_marker
def test_job():
    result = do_math_job.execute_in_process()

    # return type is ExecuteInProcessResult
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
    # inspect individual op result
    assert result.output_for_node("add_one") == 2
    assert result.output_for_node("add_two") == 3
    assert result.output_for_node("subtract") == -1


# end_test_job_marker

# start_invocation_op_marker
@op
def my_op_to_test():
    return 5


# end_invocation_op_marker

# start_test_op_marker
def test_op_with_invocation():
    assert my_op_to_test() == 5


# end_test_op_marker

# start_invocation_op_inputs_marker
@op
def my_op_with_inputs(x, y):
    return x + y


# end_invocation_op_inputs_marker

# start_test_op_with_inputs_marker
def test_inputs_op_with_invocation():
    assert my_op_with_inputs(5, 6) == 11


# end_test_op_with_inputs_marker

# start_op_requires_foo_marker
@op(required_resource_keys={"foo"})
def op_requires_foo(context):
    return f"found {context.resources.foo}"


# end_op_requires_foo_marker

# start_test_op_context_marker
from dagster import build_op_context


def test_op_with_context():
    context = build_op_context(resources={"foo": "bar"})
    assert op_requires_foo(context) == "found bar"


# end_test_op_context_marker

from dagster import resource

# start_test_resource_def_marker
@resource(config_schema={"my_str": str})
def my_foo_resource(context):
    return context.resource_config["my_str"]


def test_op_resource_def():
    context = build_op_context(resources={"foo": my_foo_resource.configured({"my_str": "bar"})})
    assert op_requires_foo(context) == "found bar"


# end_test_resource_def_marker

# start_test_job_with_config
def test_job_with_config():
    result = do_math_job.execute_in_process(
        run_config={
            "ops": {
                "add_one": {"inputs": {"num": 2}},
                "add_two": {"inputs": {"num": 3}},
            }
        }
    )

    assert result.success

    assert result.output_for_node("add_one") == 3
    assert result.output_for_node("add_two") == 5
    assert result.output_for_node("subtract") == -2


# end_test_job_with_config


# start_test_event_stream
def test_event_stream():
    job_result = emit_events_job.execute_in_process(
        run_config={"ops": {"emit_events_op": {"inputs": {"input_num": 1}}}}
    )

    assert job_result.success

    # when one op has multiple outputs, you need to specify output name
    assert job_result.output_for_node("emit_events_op", output_name="a_num") == 2

    events_for_step = job_result.events_for_node("emit_events_op")
    assert [se.event_type for se in events_for_step] == [
        DagsterEventType.STEP_START,
        DagsterEventType.STEP_INPUT,
        DagsterEventType.STEP_EXPECTATION_RESULT,
        DagsterEventType.ASSET_MATERIALIZATION,
        DagsterEventType.STEP_OUTPUT,
        DagsterEventType.HANDLED_OUTPUT,
        DagsterEventType.STEP_SUCCESS,
    ]

    # ops communicate what they did via the event stream, viewable in tools (e.g. dagit)
    (
        _start,
        _input_event,
        expectation_event,
        materialization_event,
        _num_output_event,
        _num_handled_output_operation,
        _success,
    ) = events_for_step

    # apologies for verboseness here! we can do better.
    expectation_result = expectation_event.event_specific_data.expectation_result
    assert isinstance(expectation_result, ExpectationResult)
    assert expectation_result.success
    assert expectation_result.label == "positive"

    materialization = materialization_event.event_specific_data.materialization
    assert isinstance(materialization, AssetMaterialization)
    assert materialization.label == "persisted_string"


# end_test_event_stream

# start_test_basic_asset
from dagster import asset


@asset
def basic_asset():
    return 5


# An example unit test for basic_asset.
def test_basic_asset():
    assert basic_asset() == 5


# end_test_basic_asset

# start_test_input_asset
from dagster import asset


@asset
def asset_with_inputs(x, y):
    return x + y


# An example unit test for asset_with_inputs.
def test_asset_with_inputs():
    assert asset_with_inputs(5, 6) == 11


# end_test_input_asset


# start_test_resource_asset
from dagster import asset, resource, build_op_context, with_resources


@asset(required_resource_keys={"service"})
def asset_reqs_service(context):
    service = context.resources.service
    ...


@resource
def service():
    ...


# asset_with_service now has resource service specified.
asset_with_service = with_resources([asset_reqs_service], {"service": service})[0]


def test_asset_with_service():
    # When invoking asset_with_service, service resource will
    # automatically be used.
    result = asset_with_service(build_op_context())
    ...


# end_test_resource_asset

# start_test_resource_override_asset
from dagster import resource, asset, build_op_context


@resource
def prod_resource():
    return "i am prod"


# asset_uses_service has the prod resource hardcoded to the definition.
@asset(resource_defs={"service": prod_resource})
def asset_uses_service(context):
    service = context.resources.service
    ...


def test_asset_mock_service():
    @resource
    def mock_resource():
        return "i am mock"

    # When testing, we override prod_resource with mock_resource.
    result = asset_uses_service(build_op_context(resources={"service": mock_resource}))
    ...


# end_test_resource_override_asset


def get_data_from_source():
    pass


def extract_structured_data(_):
    pass


# start_materialize_asset
from dagster import asset, materialize_in_process


@asset
def data_source():
    return get_data_from_source()


@asset
def structured_data(data_source):
    return extract_structured_data(data_source)


# An example unit test using materialize_in_process
def test_data_assets():
    result = materialize_in_process([data_source, structured_data])
    assert result.success
    # Materialized objects can be accessed in terms of the underlying op
    materialized_data = result.output_for_node("structured_data")
    ...


# end_materialize_asset


@io_manager
def source_io_manager():
    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return "foo"

    return MyIOManager()


# start_materialize_source_asset
from dagster import asset, SourceAsset, materialize_in_process, AssetKey

the_source = SourceAsset(key=AssetKey("repository_a_asset"), io_manager_def=source_io_manager)


@asset
def repository_b_asset(repository_a_asset):
    ...


@asset
def other_repository_b_asset(repository_a_asset):
    ...


def test_repository_b_assets():
    result = materialize_in_process([the_source, repository_b_asset, other_repository_b_asset])
    assert result.success
    ...


# end_materialize_source_asset

# start_materialize_resources
from dagster import asset, resource, materialize_in_process
import mock


@asset(required_resource_keys={"service"})
def asset_requires_service(context):
    service = context.resources.service
    ...


def test_asset_requires_service():
    # Mock objects can be provided directly.
    result = materialize_in_process([asset_requires_service], resources={"service": mock.MagicMock()})
    assert result.success
    ...


# end_materialize_resources
