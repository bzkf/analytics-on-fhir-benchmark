#!/bin/bash

# Number of iterations
N=20  # Change as needed

# Base seeds (can be any integers, they will be offset per iteration)
BASE_SEED=20240712

# Base output directory
BASE_OUT="./synthea/import/bulk/${BASE_SEED}"

# S3 parameters
BUCKET_NAME="analytics-on-fhir"
S3_PREFIX="synthea-import"
ENDPOINT_URL="https://nbg1.your-objectstorage.com/"  # optional, if using MinIO or self-hosted

# Check for required command
# if ! command -v aws &> /dev/null; then
#     echo "aws CLI is required but not found. Please install it first."
#     exit 1
# fi

for i in $(seq 1 $N); do
    echo "=== Run $i of $N ==="

    # Offset seeds for randomness
    SEED=$((BASE_SEED + i))
    RUN_DIR="${BASE_OUT}/run-${i}"

    # Run synthea with modified seeds and 100000 patients
    java -jar synthea-with-dependencies.jar \
        -s $SEED -cs $SEED -r $SEED \
        -p 100000 \
        -c config/synthea.properties \
        --exporter.baseDirectory="$RUN_DIR" \
        --exporter.fhir.bulk_data="true"

    # Upload to S3-compatible storage under a numbered prefix
    echo "Uploading to S3: ${S3_PREFIX}/run-${i}/"
    aws s3 cp "$RUN_DIR" "s3://${BUCKET_NAME}/${S3_PREFIX}/${i}/" \
        --recursive \
        --endpoint-url "$ENDPOINT_URL"

    # Delete the local synthea output folder
    echo "Deleting local folder: $RUN_DIR"
    # rm -rf "$RUN_DIR"

    echo "Done with run $i"
done

echo "All runs completed."
