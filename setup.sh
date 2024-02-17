# https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-google-cloud-platform
# https://github.com/google-github-actions/auth#setting-up-workload-identity-federation
# https://cloud.google.com/bigquery/docs/control-access-to-resources-iam

# Set environment variables.

# The name of the Google Cloud project.
PROJECT_ID="palpiteiro-dev"

# The name of the GitHub repository.
REPO="palpiteiro-data"

# The name of the Google Cloud service account to create.
SERVICE_ACCOUNT="github-${REPO}"

# The name of the Workload Identity Pool to create.
POOL="github-${REPO}"

# The display name of the Workload Identity Pool.
POOL_DISPLAY_NAME="github.com/${REPO}"

# The name of the Workload Identity Provider to create.
PROVIDER="github"

# The display name of the Workload Identity Provider.
PROVIDER_DISPLAY_NAME="github"

# The name of the Google BigQuery Dataset.
DATASET_ID="palpiteiro"

echo Log in using browser.
gcloud auth login --no-launch-browser

echo Set project.
gcloud config set project "${PROJECT_ID}"

echo Create a Google Cloud Service Account.
gcloud iam service-accounts create "${SERVICE_ACCOUNT}" \
    --project "${PROJECT_ID}"
echo ${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com

echo Enable the IAM Credentials API.
gcloud services enable iamcredentials.googleapis.com \
    --project "${PROJECT_ID}"

echo Create a BigQuery dataset
bq --project_id="${PROJECT_ID}" \
  mk --dataset \
  "${PROJECT_ID}:${DATASET_ID}"

echo Grant access to the service account to BigQuery Job User
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser" 

echo Grant access to the service account to BigQuery Data Editor
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

echo Get the email of the currently authenticated user
ACCOUNT=$(gcloud config get-value account)

echo Grant permissions to the user on the service account
gcloud iam service-accounts add-iam-policy-binding "${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --member="user:$ACCOUNT" \
  --role=roles/iam.serviceAccountTokenCreator

echo Grant permissions to the service account on itself
gcloud iam service-accounts add-iam-policy-binding "${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role=roles/iam.serviceAccountTokenCreator