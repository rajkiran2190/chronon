# CTR Model
Small toy model to help us test various model training and deployment integration points.

## Building
In the model dir:
```bash
$ python setup.py sdist
...
$ gsutil cp dist/test_ctr_model-1.0.tar.gz gs://zipline-warehouse-models/builds/
```

## Testing
Once the model is deployed, you can trigger an inference call via curl / GCloud's Vertex Model Web UI with the payload:
```
{
    "instances": [
      [0.75, 7500, 8.923, 1]
    ]
}
```
