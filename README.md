# Deploying to Google Cloud Run

This document provides the steps to deploy the Python application to Google Cloud Run.

## Prerequisites

1.  **Google Cloud SDK:** Make sure you have the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed and authenticated.
2.  **Enable APIs:** Enable the Cloud Build and Cloud Run APIs for your project.
    ```bash
    gcloud services enable cloudbuild.googleapis.com run.googleapis.com
    ```
3.  **Permissions:** Ensure your account has the necessary permissions to build and deploy to Cloud Run, and that the Cloud Run service account has permissions to access BigQuery.

## Deployment Steps

1.  **Set your Google Cloud Project ID:**
    ```bash
    export PROJECT_ID=$(gcloud config get-value project)
    ```

2.  **Build the Docker image using Google Cloud Build:**
    ```bash
    gcloud builds submit --tag gcr.io/$PROJECT_ID/united-data-transfer
    ```

3.  **Deploy to Cloud Run:**
    ```bash
    gcloud run deploy united-data-transfer \
        --image gcr.io/$PROJECT_ID/united-data-transfer \
        --platform managed \
        --region us-central1 \
        --allow-unauthenticated
    ```

    *   `--region`: You can choose a different region if needed.
    *   `--allow-unauthenticated`: This makes the service public. If you want to restrict access, you can use `--no-allow-unauthenticated` and set up IAM policies.

## Invoking the Service

Once deployed, you can invoke the service by sending an HTTP POST request to the URL provided by Cloud Run.

You can also trigger the function from the command line:

```bash
gcloud run services invoke united-data-transfer --region us-central1
```

## Switching the target function

The `Dockerfile` is configured to run the `copy_bigquery_table` function. If you want to run the `materialize_view_to_table` function, you need to change the `CMD` in the `Dockerfile` to:

```dockerfile
CMD ["functions-framework", "--target", "materialize_view_to_table", "--port", "8080"]
```

Then rebuild and redeploy the service.
