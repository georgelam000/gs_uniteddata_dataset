from google.cloud import bigquery
import os
import functions_framework

# --- Configuration ---
# Source table details (EU Region)
#SOURCE_PROJECT_ID = os.environ.get("SOURCE_PROJECT_ID", "gs-digital-applications-uat")
#SOURCE_DATASET_ID = os.environ.get("SOURCE_DATASET_ID", "orion")
#SOURCE_TABLE_ID = os.environ.get("SOURCE_TABLE_ID", "supply_plan_global_sourcing_readonly")

# Destination table details (Eastasia2 Region)
#DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID", "gs-digital-application-dev")
#DESTINATION_DATASET_ID = os.environ.get("DESTINATION_DATASET_ID", "orion")
#DESTINATION_TABLE_ID = os.environ.get("DESTINATION_TABLE_ID", "supply_plan_global_sourcing_readonly")

# Source table details (EU Region)
#SOURCE_PROJECT_ID = "c4-united-datasharing-prd"
#SOURCE_DATASET_ID = "products_referential"
#SOURCE_TABLE_ID = "d_bem_chain_type_rel"
SOURCE_PROJECT_ID = "gs-digital-uniteddata-prod"
SOURCE_DATASET_ID = "united_products_referential_eu"
SOURCE_TABLE_ID = "d_bem_chain_type"


# Destination table details (Eastasia2 Region)
DESTINATION_PROJECT_ID = "gs-digital-uniteddata-prod"
DESTINATION_DATASET_ID = "united_products_referential"
DESTINATION_TABLE_ID = "d_bem_chain_type"

# --- Main Function ---
@functions_framework.http
def copy_bigquery_table(request):
    """
    Triggers a cross-region BigQuery table copy job.
    Designed for HTTP-triggered deployment (e.g., Cloud Scheduler via Pub/Sub).
    """
    
    client = bigquery.Client()

    # 1. Define Source and Destination Table References
    source_ref = bigquery.TableReference(
        bigquery.DatasetReference(SOURCE_PROJECT_ID, SOURCE_DATASET_ID),
        SOURCE_TABLE_ID,
    )
    
    destination_ref = bigquery.TableReference(
        bigquery.DatasetReference(DESTINATION_PROJECT_ID, DESTINATION_DATASET_ID),
        DESTINATION_TABLE_ID,
    )

    # 2. Configure the Copy Job
    # WRITE_TRUNCATE overwrites the entire destination table every time.
    job_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # 3. Start the Copy Job
    print(f"Starting copy job from {source_ref.path} to {destination_ref.path}...")
    
    try:
        job = client.copy_table(
            source_ref,
            destination_ref,
            job_config=job_config
        )
        
        # 4. Wait for the job to complete
        job.result()  # Waits for the job to finish

        if job.errors:
            error_details = "\n".join([e['message'] for e in job.errors])
            print(f"Copy job failed with errors: {error_details}")
            return f"Error: Copy job failed with errors. Details in logs.", 500

        source_table_id = SOURCE_TABLE_ID
        destination_table_id = DESTINATION_TABLE_ID

        print(f"Successfully copied {source_table_id} to {destination_table_id}. Job ID: {job.job_id}")
        return "BigQuery table copy job succeeded!", 200

    except Exception as e:
        print(f"An exception occurred during the BigQuery copy job: {e}")
        return f"Error: BigQuery copy job failed. {e}", 500


# --- Configuration ---
# The fully qualified ID of the view you are reading from
SOURCE_VIEW = "c4-united-datasharing-prd.products_referential.d_bem_chain_type"

# The fully qualified ID of the new physical table you are creating
# NOTE: Replace 'your_project_id', 'your_dataset_id', and 'your_new_table_name'
#DESTINATION_TABLE = os.environ.get(
#    "DESTINATION_TABLE_ID",
#    "gs-digital-uniteddata-prod.united_products_referential.d_bem_chain_type_rel_eu"
#)
DESTINATION_TABLE = "d_bem_chain_typexx"
DESTINATION_DATASET = "gs-digital-uniteddata-prod.united_products_referential" 
EU_DATASET = "gs-digital-uniteddata-prod.united_products_referential_eu"

# Initialize BigQuery Client (Authentication is handled automatically by Cloud Run)
client = bigquery.Client()

@functions_framework.http
def materialize_view_to_table(request):
    
    print(f"Starting job to materialize view: {SOURCE_VIEW}")
    print(f"Destination table: {DESTINATION_TABLE}")
    
    # Materialize view to staging table in the same (EU) region
    sql_query_staging_same_region = f"""
    CREATE OR REPLACE TABLE `{EU_DATASET}.{DESTINATION_TABLE}` AS SELECT * FROM `{SOURCE_VIEW}`
    """
    # Copy from staging table (EU) to final destination table (ASIA)
    sql_query_destination = f"""
    CREATE OR REPLACE TABLE `{DESTINATION_DATASET}.{DESTINATION_TABLE}` AS SELECT * FROM `{EU_DATASET}.{DESTINATION_TABLE}`
    """

    try:
        # Transfer data from View to Table in same region (Staging)
        query_job1 = client.query_and_wait(sql_query_staging_same_region)
        
        # Check for errors in the first job (Corrected 'null' to 'None')
        if query_job1.errors is not None and len(query_job1.errors) > 0:
            error_details = "\n".join([err['message'] for err in query_job1.errors])
            print(f"Job 1 (Staging) failed with errors: {error_details}")
            return (f"BigQuery job 1 failed. Errors: {error_details}", 500)
        
        # Copy from staging table in EU to final destination region (Cross-Region)
        query_job2 = client.query_and_wait(sql_query_destination)
        
        # Check for errors in the second job (Corrected 'null' to 'None')
        if query_job2.errors is not None and len(query_job2.errors) > 0:
            error_details = "\n".join([err['message'] for err in query_job2.errors])
            print(f"Job 2 (EU to ASIA) failed with errors: {error_details}")
            return (f"BigQuery job 2 failed. Errors: {error_details}", 500)

        # Remove the staging table (Uncommented and using DROP TABLE for efficiency)
        #query_job3 = client.query_and_wait(f"DROP TABLE `{EU_DATASET}.{DESTINATION_TABLE}`")
        # NOTE: A job will be created for the DROP, but it rarely fails, so explicit error checking is often omitted.
            
        print(f"Successfully materialized view into table: {DESTINATION_TABLE}")
        # Corrected variable name from 'query_job' to 'query_job2'
        print(f"Total bytes processed (Job 2): {query_job2.total_bytes_processed}")
        
        return (f"Success: View materialized to table {DESTINATION_TABLE}", 200)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return (f"An unexpected error occurred: {e}", 500)