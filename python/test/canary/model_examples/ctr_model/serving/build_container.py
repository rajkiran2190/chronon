from google.cloud.aiplatform.prediction import LocalModel
import sys
from predictor import CTRPredictor

if len(sys.argv) != 4:
    print("Usage: python build_container.py <project_id> <repo_name> <version>")
    print("Example: python build_container.py my-project ml-models v1")
    sys.exit(1)

project_id = sys.argv[1]
repo_name = sys.argv[2]
version = sys.argv[3]

location = "us-central1"

image_uri = f"{location}-docker.pkg.dev/{project_id}/{repo_name}/ctr-predictor:{version}"

print(f"Building container: {image_uri}")

local_model = LocalModel.build_cpr_model(
    src_dir=".",
    output_image_uri=image_uri,
    predictor=CTRPredictor,
    requirements_path="requirements.txt",
    base_image="python:3.11"
)
print(f"✓ Container built successfully: {image_uri}")

print(f"\nPushing to Artifact Registry...")
local_model.push_image()

print(f"✓ Container pushed successfully!")
print(f"\nImage URI: {image_uri}")
