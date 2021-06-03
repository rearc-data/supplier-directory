#!/bin/sh
./init.sh \
  --s3-bucket norbert-adx-test2 \
  --region us-east-1 \
  --profile adx \
  --product-name supplier-directory-v2 \
  --dataset-name supplier-directory-v2 \
  --schedule-cron "cron(5 1 * * ? *)" \
  --source-url https://data.cms.gov/provider-data/sites/default/files/resources/598b68bde4da1561a570cb057a4176dd_1621267521/Medical-Equipment-Suppliers.csv \
  --product-id left-blank
