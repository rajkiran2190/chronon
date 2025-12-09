#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from gen_thrift.api.ttypes import ConfType, GroupBy, Join, Model, ModelTransforms, StagingQuery

JOIN_FOLDER_NAME = "joins"
GROUP_BY_FOLDER_NAME = "group_bys"
STAGING_QUERY_FOLDER_NAME = "staging_queries"
MODEL_FOLDER_NAME = "models"
MODEL_TRANSFORM_FOLDER_NAME = "model_transforms"
# TODO - make team part of thrift API?
TEAMS_FILE_PATH = "teams.json"
OUTPUT_ROOT = "production"

# This is set in the main function -
# from command line or from env variable during invocation
FOLDER_NAME_TO_CLASS = {
    GROUP_BY_FOLDER_NAME: GroupBy,
    JOIN_FOLDER_NAME: Join,
    STAGING_QUERY_FOLDER_NAME: StagingQuery,
    MODEL_FOLDER_NAME: Model,
    MODEL_TRANSFORM_FOLDER_NAME: ModelTransforms
}

FOLDER_NAME_TO_CONF_TYPE = {
    GROUP_BY_FOLDER_NAME: ConfType.GROUP_BY,
    JOIN_FOLDER_NAME: ConfType.JOIN,
    STAGING_QUERY_FOLDER_NAME: ConfType.STAGING_QUERY,
    MODEL_FOLDER_NAME: ConfType.MODEL,
    MODEL_TRANSFORM_FOLDER_NAME: ConfType.MODEL_TRANSFORMS
}
