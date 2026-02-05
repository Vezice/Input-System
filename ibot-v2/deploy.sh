#!/bin/bash
# iBot v2 Deployment Script
# Deploys Cloud Function to GCP

set -e

# Configuration
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-fbi-dev-484410}"
REGION="asia-southeast2"
BUCKET_NAME="${IMPORT_BUCKET:-ibot-v2-imports}"
FUNCTION_NAME="ibot-v2-import"
HTTP_FUNCTION_NAME="ibot-v2-http"

echo "=== iBot v2 Deployment ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Bucket: $BUCKET_NAME"
echo ""

# Check gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI not installed"
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1 > /dev/null 2>&1; then
    echo "Error: Not authenticated. Run 'gcloud auth login'"
    exit 1
fi

# Set project
gcloud config set project "$PROJECT_ID"

# Create bucket if it doesn't exist
echo "Checking GCS bucket..."
if ! gsutil ls -b "gs://$BUCKET_NAME" > /dev/null 2>&1; then
    echo "Creating bucket gs://$BUCKET_NAME..."
    gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
    echo "Bucket created"
else
    echo "Bucket already exists"
fi

# Deploy GCS-triggered function
echo ""
echo "Deploying GCS-triggered function: $FUNCTION_NAME..."
gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --runtime=python311 \
    --region="$REGION" \
    --source=. \
    --entry-point=process_import \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=$BUCKET_NAME" \
    --memory=512MB \
    --timeout=540s \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,IMPORT_BUCKET=$BUCKET_NAME,BIGQUERY_DATASET=ibot_v2_data,SLACK_ENABLED=true"

echo ""
echo "Deploying HTTP function: $HTTP_FUNCTION_NAME..."
gcloud functions deploy "$HTTP_FUNCTION_NAME" \
    --gen2 \
    --runtime=python311 \
    --region="$REGION" \
    --source=. \
    --entry-point=http_handler \
    --trigger-http \
    --allow-unauthenticated \
    --memory=512MB \
    --timeout=540s \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,IMPORT_BUCKET=$BUCKET_NAME,BIGQUERY_DATASET=ibot_v2_data,SLACK_ENABLED=true"

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "GCS Trigger: Files uploaded to gs://$BUCKET_NAME/{CATEGORY}/{BRAND} filename.xlsx will be processed automatically"
echo ""
echo "HTTP Endpoint:"
HTTP_URL=$(gcloud functions describe "$HTTP_FUNCTION_NAME" --region="$REGION" --format="value(serviceConfig.uri)")
echo "  Health: $HTTP_URL/health"
echo "  Process: curl -X POST $HTTP_URL/process -H 'Content-Type: application/json' -d '{\"path\": \"BA Produk LAZ/GS data.xlsx\"}'"
echo ""
echo "Test upload:"
echo "  gsutil cp test.xlsx gs://$BUCKET_NAME/BA Produk LAZ/TEST test.xlsx"
