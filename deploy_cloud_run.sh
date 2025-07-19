#!/bin/bash

# Trading Data Pipeline - Cloud Run Deployment Script
set -e

# Configuration
PROJECT_ID="ai-trading-machine"
REGION="us-central1"
SERVICE_NAME="trading-data-pipeline"
IMAGE_NAME="us-central1-docker.pkg.dev/ai-trading-machine/trading-data-pipeline-repo/trading-data-pipeline:latest"

echo "🚀 Deploying Trading Data Pipeline to Cloud Run..."

# Deploy to Cloud Run
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --memory 2Gi \
    --cpu 1 \
    --max-instances 10 \
    --timeout 3600 \
    --set-env-vars GOOGLE_CLOUD_PROJECT=$PROJECT_ID \
    --set-env-vars ENVIRONMENT=production \
    --project $PROJECT_ID

echo "✅ Deployment completed!"

# Get the service URL
echo "📋 Service URL:"
gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)'
