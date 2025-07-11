#!/bin/bash

# Number of iterations
N=20  # Must match what you used in the synthea generation script

# S3 parameters
BUCKET_NAME="analytics-on-fhir"
S3_PREFIX="synthea-import"
ENDPOINT_URL="https://nbg1.your-objectstorage.com/"  # optional, if using MinIO or self-hosted

# FHIR $import endpoint (e.g., https://fhir.example.com/fhir/$import)
IMPORT_ENDPOINT='http://localhost:8082/fhir/$import'

# Optional: endpoint for storage if your FHIR server needs access details
# STORAGE_HEADERS="Authorization: Bearer <your-token>"

S3_BASE_URL="s3a://${BUCKET_NAME}/${S3_PREFIX}"

for i in $(seq 1 $N); do
  echo "=== Importing run $i ==="

  # Construct S3 URLs for Patient and Observation resources
  PATIENT_URL="${S3_BASE_URL}/${i}/fhir/Patient.ndjson"
  OBSERVATION_URL="${S3_BASE_URL}/${i}/fhir/Observation.ndjson"

  # Create the JSON import payload
  IMPORT_PAYLOAD=$(cat <<EOF
{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Patient"
        },
        {
          "name": "url",
          "valueUrl": "${PATIENT_URL}"
        },
        {
          "name": "mode",
          "valueCode": "merge"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Observation"
        },
        {
          "name": "url",
          "valueUrl": "${OBSERVATION_URL}"
        },
        {
          "name": "mode",
          "valueCode": "merge"
        }
      ]
    }
  ]
}
EOF
)

  # Send the $import request
  echo "Sending import request for run ${i}..."
  curl --retry 10 --retry-all-errors -X POST "$IMPORT_ENDPOINT" \
    -H "Content-Type: application/fhir+json" \
    -d "${IMPORT_PAYLOAD}"

  echo "Done with run $i"
done

echo "All import requests sent."
