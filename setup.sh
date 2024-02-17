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
gcloud auth login

echo Set project.
gcloud config set project "${PROJECT_ID}"

echo Create a Google Cloud Service Account.
gcloud iam service-accounts create "${SERVICE_ACCOUNT}" \
    --project "${PROJECT_ID}"
echo ${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com

echo Enable the IAM Credentials API.
gcloud services enable iamcredentials.googleapis.com \
    --project "${PROJECT_ID}"

echo Enable the Cloud Resource Manager API.
gcloud services enable cloudresourcemanager.googleapis.com \
    --project "${PROJECT_ID}"

echo Create a Workload Identity Pool.
gcloud iam workload-identity-pools create "${POOL}" \
    --project="${PROJECT_ID}" \
    --location="global" \
    --display-name="${POOL_DISPLAY_NAME}"

echo Get the full ID of the Workload Identity Pool.
WORKLOAD_IDENTITY_POOL_ID=$(gcloud iam workload-identity-pools describe "${POOL}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --format="value(name)")
echo ${WORKLOAD_IDENTITY_POOL_ID}

echo Create a Workload Identity Provider in that pool.
gcloud iam workload-identity-pools providers create-oidc "${PROVIDER}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${POOL}" \
  --display-name="${PROVIDER_DISPLAY_NAME}" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"

echo Allow authentications from the Workload Identity Provider originating from your repository to impersonate the Service Account created above.
gcloud iam service-accounts add-iam-policy-binding "${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${REPO}"

echo Extract the Workload Identity Provider resource name.
gcloud iam workload-identity-pools providers describe "${PROVIDER}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${POOL}" \
  --format="value(name)"

echo Create a BigQuery dataset
bq --project_id="${PROJECT_ID}" \
  mk --dataset \
  "${PROJECT_ID}:${DATASET_ID}"

echo Grant access to the service account to create tables and jobs in the BigQuery dataset
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor" \
  --role="roles/bigquery.jobUser" \
  --role="roles/iam.serviceAccountTokenCreator"
