## Prerequisites
* azure CLI
* Docker
* For GCP authentication testing you need to have a Service Account JSON key file.
* For signed URL testing you need to generate one for GCS/S3 or Azure Blob Storage.
```bash
gcloud storage sign-url gs://polars-bio-it/vep.vcf.bgz --impersonate-service-account=polars-bio-it@sequila-native-testing.iam.gserviceaccount.com --duration=12h --region=europe-west1
```
### Start the environment
```bash
source bin/start.sh
```

### Run object storage IO tests
```bash
pytest it_object_storage_io.py -o log_cli=true --log-cli-level=INFO
```

### Stop/cleanup the environment
```bash
bin/stop.sh
```