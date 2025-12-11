# CTR Model
Small toy model to help us test various model training and deployment integration points.

## Building training Python src distribution
In the model dir:
```bash
$ python setup.py sdist
...
$ gsutil cp dist/test_ctr_model-1.0.tar.gz gs://zipline-warehouse-models/builds/
```

## Building Custom Serving container

We use [Vertex's Custom Prediction Routines](https://docs.cloud.google.com/vertex-ai/docs/predictions/custom-prediction-routines) to help build a custom serving container. This is primarily to help us use customized input / output mapping for inbound requests for inference and model outputs.
To build the container:
```bash
$ cd serving
$ export DOCKER_DEFAULT_PLATFORM=linux/amd64
$ python build_container.py canary-443022 canary-images v1
...
✓ Container built successfully: us-central1-docker.pkg.dev/canary-443022/canary-images/ctr-predictor:v1
Pushing to Artifact Registry...
✓ Container pushed successfully!

Image URI: us-central1-docker.pkg.dev/canary-443022/canary-images/ctr-predictor:v1
```

This image URI is what we have set in the test/canary/models/gcp/click_through_rate.py file under the DeploymentSpec.

## Testing
Once the model is deployed, you can trigger an inference call via curl / GCloud's Vertex Model Web UI. URL looks like:
`https://us-central1-aiplatform.googleapis.com/v1/projects/canary-443022/locations/us-central1/endpoints/$endpointID:predict`

Payload:
```
{
  "instances": [
     {"user_id_click_event_average_7d": 0.9, "listing_price_cents": 50000, "price_log": 10.820, "price_bucket": 3}
   ]
}
```

Response:
```
{
    "predictions": [
        {
            "score": 0.42078027129173279
        }
    ],
    "deployedModelId": "3160575860856061952",
    "model": "projects/703996152583/locations/us-central1/models/1697196253030383616",
    "modelDisplayName": "test_ctr_model-1.0",
    "modelVersionId": "1"
}
```
