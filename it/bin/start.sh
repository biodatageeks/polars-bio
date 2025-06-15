#!/usr/bin/env bash
set -a
source bin/.env

#################### S3 Setup

docker-compose -f docker-compose.yml up -d
sleep 5

### Set alias
docker exec -it minio mc alias set local http://host.docker.internal:9000 test_user test_secret
##
### Create bucket
docker exec -it minio mc mb local/polarsbio
##
### Create a public bucket
docker exec -it minio mc mb local/polarsbiopublic
docker exec -it minio mc anonymous set public local/polarsbiopublic

### Upload files
docker exec -it minio mc mirror "/test_data/" local/polarsbio


docker exec -it minio mc mirror "/test_data/" local/polarsbiopublic

docker exec -it minio mc admin user add local  $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY

### policies
docker exec -it minio mc admin policy create local polarsbio-readonly /test_data/policy-priv.json
#docker exec -it minio mc admin policy create local polarsbiopublic-anonymous /test_data/policy-anonymous.json

docker exec -it minio mc admin policy attach local polarsbio-readonly --user=$AWS_ACCESS_KEY_ID

docker exec -it minio mc anonymous set-json /test_data/policy-anonymous.json local/polarsbiopublic


########### Azure blob storage
docker run -p 10000:10000 --name azurite -d  mcr.microsoft.com/azure-storage/azurite

az storage container create \
  --name polarsbio \
  --account-name $AZURE_STORAGE_ACCOUNT \
  --account-key $AZURE_STORAGE_KEY \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=$AZURE_STORAGE_ACCOUNT;AccountKey=$AZURE_STORAGE_KEY;BlobEndpoint=$AZURE_ENDPOINT_URL;"


az storage blob upload \
  --container-name polarsbio \
  --name vep.vcf.bgz\
  --file data/vep.vcf.bgz \
  --account-name $AZURE_STORAGE_ACCOUNT \
  --account-key $AZURE_STORAGE_KEY \
  --overwrite \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=$AZURE_STORAGE_ACCOUNT;AccountKey=$AZURE_STORAGE_KEY;BlobEndpoint=$AZURE_ENDPOINT_URL;"

az storage blob list \
  --container-name polarsbio \
  --account-name $AZURE_STORAGE_ACCOUNT \
  --account-key $AZURE_STORAGE_KEY \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=$AZURE_STORAGE_ACCOUNT;AccountKey=$AZURE_STORAGE_KEY;BlobEndpoint=$AZURE_ENDPOINT_URL;" \
  --output table
