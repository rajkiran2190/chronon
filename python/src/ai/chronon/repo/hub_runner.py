import json
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import click
import requests
from gen_thrift.planner.ttypes import Mode

from ai.chronon.cli.formatter import Format, format_print, jsonify_exceptions_if_json_format
from ai.chronon.cli.git_utils import get_current_branch
from ai.chronon.click_helpers import handle_compile, handle_conf_not_found
from ai.chronon.repo import hub_uploader, utils
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.utils import print_possible_confs
from ai.chronon.repo.zipline_hub import ZiplineHub
from ai.chronon.schedule_validation import validate_at_most_daily_schedule

ALLOWED_DATE_FORMATS = ["%Y-%m-%d"]

DEFAULT_TEAM_METADATA_CONF = "compiled/teams_metadata/default/default_team_metadata"

@dataclass
class HubConfig:
    hub_url: str
    frontend_url: str
    sa_name: Optional[str] = None
    eval_url: Optional[str] = None
    fetcher_url: Optional[str] = None
    cloud_provider: Optional[str] = None
    artifact_prefix: Optional[str] = None


@dataclass
class ScheduleModes:
    offline_schedule: str
    online_schedule: str

@click.group()
def hub():
    pass

def repo_option(func):
    return click.option("--repo", help="Path to chronon repo", default=".")(func)
def use_auth_option(func):
    return click.option(
        "--use-auth/--no-use-auth", help="Use authentication when connecting to Zipline Hub", default=True
    )(func)
def hub_url_option(func):
    return click.option(
        "--hub_url", help="Zipline Hub address, e.g. http://localhost:3903", default=None
    )(func)
def format_option(func):
    return click.option(
        "--format", help="Format of the response", default=Format.TEXT, type=click.Choice(Format, case_sensitive=False)
    )(func)
def force_option(func):
    return click.option(
        "--force", help="Force compile even if there are version changes to existing confs", is_flag=True
    )(func)


def get_conf_type(conf):
    if "compiled/joins" in conf:
        return "joins"
    elif "compiled/staging_queries" in conf:
        return "stagingqueries"
    elif "compiled/group_by" in conf:
        return "groupbys"
    elif "compiled/models" in conf:
        return "models"
    elif "compiled/model_transforms" in conf:
        return "modeltransforms"
    else:
        raise ValueError(f"Unsupported conf type: {conf}")

#### Common click options
def common_options(func):
    func = repo_option(func)
    func = click.option("--conf", required=True, help="Conf param - required for every mode")(func)
    func = hub_url_option(func)
    func = use_auth_option(func)
    func = format_option(func)
    func = force_option(func)
    return func


def ds_option(func):
    return click.option(
        "--ds",
        help="the end partition to backfill the data",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
    )(func)


def start_ds_option(func):
    return click.option(
        "--start-ds",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        help="override the original start partition for a range backfill. "
        "It only supports staging query, group by backfill and join jobs. "
        "It could leave holes in your final output table due to the override date range.",
    )(func)

def workflow_id_option(func):
    return click.option(
        "--workflow-id",
        help="Zipline workflow id",
        type=str,
        required=True,
    )(func)


def end_ds_option(func):
    return click.option(
        "--end-ds",
        help="the end ds for a range backfill",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        default=str(date.today() - timedelta(days=2)),
    )(func)

def _get_zipline_hub(hub_url: Optional[str], hub_conf: Optional[HubConfig], use_auth: bool, format: Format = Format.TEXT):
    if hub_url is not None:
        zipline_hub = ZiplineHub(base_url=hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth, cloud_provider=hub_conf.cloud_provider, format=format)
    else:
        zipline_hub = ZiplineHub(base_url=hub_conf.hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth, cloud_provider=hub_conf.cloud_provider, format=format)
    return zipline_hub

def submit_workflow(repo, conf, mode, start_ds, end_ds, hub_url=None, use_auth=True, format: Format = Format.TEXT):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict, format=format
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)

    response_json = zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=mode,
        branch=branch,  # Get the current branch
        user=os.environ.get("USER"),
        start=start_ds,
        end=end_ds,
        conf_hash=conf_name_to_hash_dict[conf_name].hash,
        skip_long_running=False,
    )

    workflow_id = response_json.get("workflowId", "N/A")
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)
    format_print(f" 🆔 Workflow Id: {workflow_id}", format=format)
    print_wf_url(
        conf=conf,
        conf_name=conf_name,
        mode=mode,
        workflow_id=workflow_id,
        repo=repo,
        format=format
    )


def submit_schedule(repo, conf, hub_url=None, use_auth=True, format: Format = Format.TEXT):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth, format)
    conf_name_to_obj_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_obj_dict, format=format
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    schedule_modes = get_schedule_modes(os.path.join(repo, conf))
    # create a dict for RunMode.BACKFILL.value and RunMode.DEPLOY.value to schedule_modes.offline_schedule and schedule_modes.online_schedule
    modes = {
        RunMode.BACKFILL.value.upper(): schedule_modes.offline_schedule,
        RunMode.DEPLOY.value.upper(): schedule_modes.online_schedule,
    }
    response_json = zipline_hub.call_schedule_api(
        modes=modes,
        branch=branch,
        conf_name=conf_name,
        conf_hash=conf_name_to_obj_dict[conf_name].hash,
    )
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)

    schedules = response_json.get("schedules", "N/A")
    readable_schedules = {Mode._VALUES_TO_NAMES[int(k)]: v for k, v in schedules.items()}
    format_print(f" 🗓️ Schedules Deployed: {readable_schedules}", format=format)


# zipline hub backfill --conf=compiled/joins/join
# adhoc backfills
@hub.command()
@common_options
@start_ds_option
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def backfill(repo, conf, hub_url, use_auth, format, force, start_ds, end_ds, skip_compile):
    """
    - Submit a backfill job to Zipline.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to backfill.
    """
    submit_workflow(
        repo, conf, RunMode.BACKFILL.value, start_ds, end_ds, hub_url=hub_url, use_auth=use_auth, format=format
    )


# zipline hub run-adhoc --conf=compiled/joins/join
# currently only supports one-off deploy node submission
@hub.command()
@common_options
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def run_adhoc(repo, conf, hub_url, use_auth, format, force, end_ds, skip_compile):
    """
    - Submit a one-off deploy job to Zipline. This submits the various jobs to allow your conf to be tested online.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to deploy
    """
    submit_workflow(repo, conf, RunMode.DEPLOY.value, end_ds, end_ds, hub_url=hub_url, use_auth=use_auth, format=format)


# zipline hub schedule --conf=compiled/joins/join
@hub.command()
@common_options
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def schedule(repo, conf, hub_url, use_auth, format, force, skip_compile):
    """
    - Deploys a schedule for the specified conf to Zipline. This allows your conf to have various associated jobs run on a schedule.
    This verb will introspect your conf to determine which of its jobs need to be scheduled (or paused if turned off) based on the
    'offline_schedule' and 'online' fields.
    """
    submit_schedule(repo, conf, hub_url=hub_url, use_auth=use_auth, format=format)

@hub.command()
@repo_option
@hub_url_option
@use_auth_option
@workflow_id_option
@jsonify_exceptions_if_json_format
def cancel(repo, hub_url, use_auth, format, force, workflow_id):
    zipline_hub = _get_zipline_hub(hub_url, get_hub_conf_from_metadata_conf(DEFAULT_TEAM_METADATA_CONF, root_dir=repo), use_auth, format)
    response_json = zipline_hub.call_cancel_api(workflow_id, format=format)
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)
    format_print(f" 🟢 Workflow cancelled: {workflow_id}", format=format)

def load_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data

def get_metadata_map(file_path):
    data = load_json(file_path)
    metadata_map = data["metaData"]
    return metadata_map


def get_common_env_map(file_path, skip_metadata_extraction=False):
    metadata_map = get_metadata_map(file_path) if not skip_metadata_extraction else load_json(file_path)
    common_env_map = metadata_map["executionInfo"]["env"]["common"]
    return common_env_map


# zipline hub fetch --conf=compiled/joins/join
# call the zipline fetcher from the hub
@hub.command()
@common_options
@click.option(
    "--fetcher-url",
    help="Fetcher Server",
    type=str,
    default=None
)
@click.option(
    "--schema",
    help="Get only the schema",
    is_flag=True,
)
@click.option(
    "--key-json",
    help="Json of the keys to fetch",
    type=str,
    default=None
)
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@jsonify_exceptions_if_json_format
def fetch(repo, conf, hub_url, use_auth, format, force, fetcher_url, schema, key_json):
    """
    - Fetch data from the fetcher server.
    - If schema is True, fetch the schema of the join.
    - If schema is False, fetch the data of the join.
    """
    hub_conf = get_hub_conf(conf, root_dir=repo)
    fetcher_url = fetcher_url or hub_conf.fetcher_url
    r = requests.get(f"{fetcher_url}/ping", timeout=100)
    if r.status_code != 200:
        raise RuntimeError(f"Fetcher server is not running. Please start the fetcher server and try again. Url: {fetcher_url}/ping Status code: {r.status_code}")
    # Figure out if it's a group by or join
    conf_type = get_conf_type(conf)
    target = utils.get_metadata_name_from_conf(repo, conf)

    # TODO: fix this workaround to just use conf_type directly singular
    if conf_type == "modeltransforms":
        endpoint = "/v1/fetch/{conf_type}".format(conf_type=conf_type)
    else:
        endpoint = "/v1/fetch/{conf_type}".format(conf_type=conf_type[:-1])
    if schema:
        if conf_type != "joins":
            raise ValueError("Schema fetch is only supported for joins")
        endpoint = f"/v1/join/{target}/schema"
    headers = {"Content-Type": "application/json"}
    try:
        if schema:
            url = f"{fetcher_url}{endpoint}"
            response = requests.get(url, headers=headers, timeout=100)
        else:
            url = f"{fetcher_url}{endpoint}/{target}"
            key_json = json.loads(key_json)
            response = requests.post(url, headers=headers, json=key_json, timeout=100)
        if response.status_code != 200:
            raise requests.RequestException(f"Request failed: {url} with status code: {response.status_code}\nResponse: {response.text}")
        print(json.dumps(response.json(), indent=4))
    except requests.RequestException as e:
        raise RuntimeError(f"""
        Request failed for url: {url}
        The conditions for a successful fetch are:
        - Metadata has been uploaded to the KV Store (run-adhoc command or schedule command)
        - The join needs to be online.
        Please verify the above conditions and try again.
        Error: {e}
        """) from e

# zipline hub eval --conf=compiled/joins/join
# localSparkSession evaluation of conf
@hub.command()
@common_options
@click.option(
    "--eval-url",
    help="Eval Server",
    type=str,
    default=None
)
@click.option(
    "--generate-test-config",
    help="Generate a test config for data testing",
    is_flag=True,
    default=None
)
@click.option(
    "--test-data-path",
    help="Path to the test data yaml file to upload and use for evaluation.",
    type=str,
    default=None
)
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
@jsonify_exceptions_if_json_format
def eval(repo, conf, hub_url, use_auth, format, force, eval_url, generate_test_config, test_data_path, skip_compile):
    """
    - Submit a eval job to Zipline.
    Response should contain a list of validation checks that are executed in a sparkLocalSession with Metadata access.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual eval API.
    """
    parameters = {}
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = ZiplineHub(base_url=hub_url or hub_conf.hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth, eval_url=eval_url or hub_conf.eval_url, cloud_provider=hub_conf.cloud_provider, format=format)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()
    if test_data_path:
        # Upload the test data skeleton to the bucket.
        if hub_conf.cloud_provider != "gcp":
            raise RuntimeError(" 🔴 Test data path is only supported for GCP")
        # import here to avoid dependency for other clouds.
        from ai.chronon.repo.gcp import GcpRunner
        zipline_artifact_prefix = hub_conf.artifact_prefix
        if not zipline_artifact_prefix:
            print(" 🔴 Zipline artifact prefix is not set")
            sys.exit(1)
        url = f"eval/test_data/{os.path.basename(test_data_path)}"
        GcpRunner.upload_gcs_blob(zipline_artifact_prefix.replace("gs://", ""), test_data_path, url)
        parameters["testDataPath"] = f"{zipline_artifact_prefix}/{url}"

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    if generate_test_config:
        parameters["generateTestDataSkeleton"] = "true"
    response_json = zipline_hub.call_eval_api(
        conf_name=conf_name,
        conf_hash_map={conf.name: conf.hash for conf in conf_name_to_hash_dict.values()},
        parameters=parameters,
    )
    if response_json.get("success"):
        format_print(" 🟢 Eval job finished successfully", format=format)
        format_print(response_json.get("message"), format=format)
    else:
        format_print(" 🔴 Eval job failed", format=format)
        format_print(response_json.get("message"), format=format)
        sys.exit(1)
    if format == Format.JSON:
        print(json.dumps(response_json, indent=4))
        sys.exit(0)


def get_hub_conf(conf_path, root_dir="."):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, conf_path)
    common_env_map = get_common_env_map(file_path)
    common_env_map.update(os.environ) # Override config with cli args
    kwargs = {k: common_env_map.get(k.upper()) for k in HubConfig.__dataclass_fields__.keys()}
    return HubConfig(**kwargs)

def get_hub_conf_from_metadata_conf(metadata_path, root_dir="."):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, metadata_path)
    common_env_map = get_common_env_map(file_path, skip_metadata_extraction=True)
    common_env_map.update(os.environ) # Override config with cli args
    hub_url = common_env_map.get("HUB_URL")
    frontend_url = common_env_map.get("FRONTEND_URL")
    sa_name = common_env_map.get("SA_NAME")
    eval_url = common_env_map.get("EVAL_URL")
    return HubConfig(hub_url=hub_url, frontend_url=frontend_url, sa_name=sa_name, eval_url=eval_url)


def get_schedule_modes(conf_path):
    metadata_map = get_metadata_map(conf_path)
    # Get the online and offline schedules from executionInfo
    online_schedule = metadata_map["executionInfo"].get("onlineSchedule", None)
    offline_schedule = metadata_map["executionInfo"].get("offlineSchedule", None)

    # Validate schedule expressions using croniter-based validation
    if offline_schedule:
        validation_error = validate_at_most_daily_schedule(offline_schedule)
        if validation_error:
            raise ValueError(f"Invalid offline_schedule: {validation_error}")

    if online_schedule:
        validation_error = validate_at_most_daily_schedule(online_schedule)
        if validation_error:
            raise ValueError(f"Invalid online_schedule: {validation_error}")

    # Default to "None" string if schedules are not set
    # This is used by the schedule API to determine if a schedule is active
    online_schedule = online_schedule or "None"
    offline_schedule = offline_schedule or "None"
    return ScheduleModes(offline_schedule=offline_schedule, online_schedule=online_schedule)


def print_wf_url(conf, conf_name, mode, workflow_id, repo=".", format: Format = Format.TEXT):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    frontend_url = hub_conf.frontend_url
    hub_conf_type = get_conf_type(conf)

    def _mode_string():
        if mode == "backfill":
            return "offline"
        elif mode == "deploy":
            return "online"
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    workflow_url = f"{frontend_url.rstrip('/')}/{hub_conf_type}/{conf_name}/{_mode_string()}?workflowId={workflow_id}"

    format_print(" 🔗 Workflow : " + workflow_url + "\n", format=format)

if __name__ == "__main__":
    hub()
