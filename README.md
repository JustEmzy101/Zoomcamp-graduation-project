# Data Orchestration & ETL Platform on Kubernetes

A comprehensive, production-grade data orchestration and ETL platform deployed on **Kubernetes**, integrating Apache Airflow, Airbyte, Apache Spark, and Google Cloud Platform (GCP).

---

## 🎯 Project Overview

This project provides a complete, enterprise-ready data pipeline infrastructure running entirely on **Kubernetes**, enabling:

- **Kubernetes-Native Orchestration**: Apache Airflow with KubernetesExecutor for dynamic pod scaling and resource efficiency
- **Cloud-Native Data Integration**: Airbyte ELT with containerized connectors and state management via MinIO
- **Distributed Computing**: Apache Spark Operator with SparkApplication CRDs for large-scale data processing
- **Multi-Tenant Ready**: Isolated namespaces for Airflow, Airbyte, and Spark workloads
- **GCP Integration**: Native connectivity to BigQuery and Google Cloud Storage
- **Network Isolation**: Tailscale VPN integration for secure developer access to the cluster

The platform is designed for enterprise-grade ETL/ELT operations with high scalability, reliability, and security.

---

## 🏗️ Kubernetes Architecture

This platform leverages **Kubernetes** as the foundational infrastructure with multi-namespace isolation and StatefulSet/Deployment patterns.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     KUBERNETES CLUSTER (Multi-Namespace)                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────────┐  ┌──────────────────┐  ┌──────────────────────┐ │
│  │  airflow Namespace  │  │ airbyte Namespace│  │ spark Namespace      │ │
│  │                     │  │                  │  │                      │ │
│  │ ┌─────────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────────┐ │ │
│  │ │  Airflow Pods   │ │  │ │ Airbyte Pods │ │  │ │  Spark Jobs      │ │ │
│  │ │ (Scheduler, WM) │ │  │ │  (Server, WL)│ │  │ │ (SparkApps CRD) │ │ │
│  │ └────────┬────────┘ │  │ └──────┬───────┘ │  │ └────────┬─────────┘ │ │
│  │          │          │  │        │         │  │          │           │ │
│  │  PVC: logs & dags  │  │ MinIO  │ Redis   │  │ S3: temp  │          │ │
│  └──────────┼──────────┘  │(State) │ (Cache) │  └──────────┼──────────┘ │
│             │             └────┬───┴────┬─────┘             │            │
│             │                  │        │                   │            │
│  ┌──────────┼──────────────────┼────────┼───────────────────┼──────────┐ │
│  │          │ PostgreSQL DB    │        │                   │          │ │
│  │  Airflow │ (airflow_db) ◄───┤        │                   │          │ │
│  │          │                  │        │                   │          │ │
│  │ Airbyte  │ PostgreSQL DB    │        │                   │          │ │
│  │          │ (airbyte_db) ◄───┤        │                   │          │ │
│  │          │                  │        │                   │          │ │
│  │  Shared: NetworkPolicy, RBAC, ConfigMaps, Secrets        │          │ │
│  └──────────────────────────────────────────────────────────┴──────────┘ │
│                                                                            │
│  RBAC & Security:                                                         │
│  • ClusterRole: spark-operator-clusterrole (CRD management)              │
│  • Roles: airflow-worker-role, spark-job-role                            │
│  • ServiceAccounts: namespace-scoped access                              │
│  • Secrets: GCP credentials, HMAC tokens                                 │
└──────────────────────────────────────────────────────────────────────────┘
         │
         ├─────────────────────────────────────────────┐
         │                                             │
         │                                             ▼
    ┌────▼─────────────────┐              ┌────────────────────┐
    │  Tailscale VPN (VPC) │              │       GCP          │
    │                      │              ├────────────────────┤
    │  • Developer Access  │◄────────────►│ • BigQuery (DWH)   │
    │  • Secure Tunneling  │              │ • Cloud Storage    │
    │  • Network Isolation │              │ • Service Accounts │
    └──────────────────────┘              └────────────────────┘
```

### Core Components

1. **Apache Airflow** (v3.1.7) - Kubernetes Orchestration
   - KubernetesExecutor: Dynamic pod creation per task
   - Separate PostgreSQL DB (`airflow_db`)
   - Persistent Volume for DAGs and logs
   - NodePort service on port 32080

2. **Airbyte** (Community Edition) - ELT Pipeline
   - Separate PostgreSQL DB (`airbyte_db`)
   - MinIO container for job state files
   - Workload API server (port 8007)
   - Workload Launcher for distributed syncs

3. **Apache Spark Operator** - Distributed Computing
   - SparkApplication CRD resources
   - Python-based Spark jobs (v3.4.0)
   - GCS and BigQuery connectors
   - Automatic pod scaling

4. **Tailscale VPN** - Secure Network Layer
   - Virtual Private Network between developer machines and cluster
   - Encrypted tunneling for Kubernetes API access
   - No public IP exposure required
   - Zero-trust networking model

5. **Google Cloud Platform** - Cloud Infrastructure
   - BigQuery for analytics and data warehousing
   - Cloud Storage for data lakes
   - Service accounts with fine-grained IAM roles
   - HMAC credentials for storage authentication

---

## 📁 Project Structure

```
project/
├── README.md                              # This file
├── LICENSE                                # Project license
│
├── 🔧 Kubernetes Configuration Files
├── airflow-values.yaml                    # Airflow Helm values
├── airflow-worker-role.yaml               # RBAC for Airflow workers
├── airflow-spark-role.yaml                # RBAC for Spark access
├── airflow-spark-rbac-and-rolebinding.yaml # Combined RBAC/RoleBinding
├── airflow-spark-rolebinding.yaml         # RoleBinding config
├── airflow-ui-id.txt                      # Airflow service ID reference
│
├── airbyte-values.yaml                    # Airbyte Helm values
├── airbyte-cred                           # Airbyte credentials
├── airbyte-webapp-service.yaml            # Airbyte service definition
├── workload-api-alias.yaml                # Airbyte workload API alias
│
├── spark-operator-clusterrole.yaml        # Spark Operator RBAC
├── spark-pi.yaml                          # Example Spark job
│
├── 📂 airflow-logs-pv.yaml                # Persistent Volume for logs
├── airflow-logs-pvc.yaml                  # Persistent Volume Claim
│
├── 📂 airflow-image/                      # Custom Airflow Docker image
│   └── Dockerfile                         # Multi-provider Airflow setup
│
├── 📂 spark-dockerfile/                   # Custom Spark Docker image
│   └── Dockerfile                         # Spark application Docker setup
│
├── 📂 spark-apps/                         # Spark applications
│   └── gcs-to-bq.yaml                     # GCS → BigQuery Spark job
│
└── 📂 keys/                               # Credentials and keys
    ├── airbyte-sa-key.json                # Airbyte service account key
    ├── spark-etl-key.json                 # Spark ETL service account key
    └── HMAC.txt                           # HMAC credentials for GCS
```

---

## 🚀 Key Features

### Workflow Orchestration
- Multi-step DAGs with dynamic task generation
- KubernetesExecutor for resource-efficient scaling
- Task dependencies and error handling
- Monitoring and alerting capabilities

### Data Integration
- Airbyte ELT connectors (100+ data sources)
- Automated schema discovery
- Incremental data loading
- Data validation and transformation

### Distributed Computing
- Spark jobs for large-scale processing
- GCS data lake connectivity
- BigQuery direct writes
- Automatic dependency management

### Cloud Integration
- Native GCP authentication
- BigQuery integration for analytics
- GCS for cost-effective storage
- Service account-based security

---

## 🔐 Credentials Management

### Setting Up GCP Service Account Keys

#### For Airbyte Service Account

Create a `keys/airbyte-sa-key.json.example` template:

```json
{
  "type": "service_account",
  "project_id": "your-gcp-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "airbyte-sa@your-gcp-project-id.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

**How to generate**:
1. Go to Google Cloud Console → IAM & Admin → Service Accounts
2. Create a new service account: `airbyte-etl`
3. Grant roles:
   - `roles/bigquery.dataEditor`
   - `roles/storage.objectAdmin`
   - `roles/bigquery.jobUser`
    ** These Role should be tighten abit since they are very wide and can be for a certain bucket/table
4. Create a key (JSON format)
5. Download and save as `keys/airbyte-sa-key.json`

#### For Spark ETL Service Account

Create a `keys/spark-etl-key.json.example` template (same format as above):

```json
{
  "type": "service_account",
  "project_id": "your-gcp-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "spark-etl@your-gcp-project-id.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

**How to generate**:
1. Go to Google Cloud Console → IAM & Admin → Service Accounts
2. Create a new service account: `spark-etl`
3. Grant roles:
   - `roles/bigquery.dataEditor`
   - `roles/storage.objectAdmin`
   - `roles/bigquery.jobUser`
   ** These Role should be tighten abit since they are very wide and can be for a certain bucket/table
4. Create a key (JSON format)
5. Download and save as `keys/spark-etl-key.json`

#### For GCS HMAC Credentials

Create a `keys/HMAC.txt.example` template:

```
GOOGCX1234567890ABCDEFGH
Aw4kGc7X1q2w3e4r5t6y7u8i9o0p1a2s
```

**How to generate**:
1. Go to Google Cloud Console → Cloud Storage → Settings
2. Go to "Interoperability" tab
3. Click "Create a new key" in the "Service account HMAC" section
4. Copy the **Access Key** and **Secret** 
5. Save to `keys/HMAC.txt` (format: AccessKey\nSecretKey, one per line)

### Secret Creation Summary

```bash
# Create the secrets in Kubernetes after populating credential files:

# Airflow namespace secrets
kubectl create secret generic gcp-airflow-keys \
  --from-file=airbyte-sa.json=keys/airbyte-sa-key.json \
  --from-file=spark-etl.json=keys/spark-etl-key.json \
  -n airflow

# Airbyte namespace secrets
kubectl create secret generic gcp-airbyte-keys \
  --from-file=sa-key.json=keys/airbyte-sa-key.json \
  -n airbyte

# Spark namespace secrets
kubectl create secret generic gcp-spark-keys \
  --from-file=sa-key.json=keys/spark-etl-key.json \
  --from-file=hmac.txt=keys/HMAC.txt \
  -n spark

# Verify secrets are created
kubectl get secrets --all-namespaces | grep gcp
```

---

## 🔌 Tailscale VPN Integration (Secure Developer Access)

### Overview

Tailscale acts as a **Virtual Private Cloud (VPC)** between your developer machines and the Kubernetes cluster, providing:

- **Zero Trust Networking**: Encrypted point-to-point connections
- **No Port Exposure**: Cluster APIs and services not exposed to public internet
- **Developer-First Access**: Each developer gets secure access to cluster resources
- **Network Isolation**: Separate from production GCP networks

### Architecture

```
Developer Laptop              Tailscale Network              Kubernetes Cluster
┌─────────────────────────┐                           ┌──────────────────────────┐
│                         │                           │                          │
│ ┌──────────────────────┐│  Encrypted Tunnel  ┌─────┤ Tailscale Daemon Pod     │
│ │ Tailscale Client     ││◄──────────────────►│ TS  │ (Exit Node)              │
│ │ (VPN Enabled)        ││                    │ TUN │                          │
│ └──────────────────────┘│                    └─────┤ ┌──────────────────────┐ │
│                         │                          │ │ Kubernetes Services  │ │
│ kubectl port-forward    │                          │ │ DNS: *.cluster.local │ │
│ helm connect            │                          │ │ Pod IPs: 10.0.0.x    │ │
│ curl http://svc:8080    │                          │ └──────────────────────┘ │
│                         │                          │                          │
└─────────────────────────┘                          └──────────────────────────┘
```

### Setup Steps

1. **Create Tailscale Auth Key**:
   - Visit https://login.tailscale.com/admin/keys
   - Generate an auth key (reusable, 90 days expiry)
   - Select "OAuth" or "Pre-authenticated"

2. **Deploy Tailscale to Kubernetes**:
   ```bash
   # Create Tailscale namespace
   kubectl create namespace tailscale
   
   # Create secret with auth key
   kubectl create secret generic tailscale-auth \
     --from-literal=TS_AUTHKEY=tskey-k123456... \
     -n tailscale
   
   # Apply Tailscale DaemonSet (requires tailscale-daemonset.yaml)
   kubectl apply -f tailscale-daemonset.yaml
   ```

3. **Configure Developer Machine**:
   ```bash
   # Install Tailscale on your laptop
   # macOS:
   brew install tailscale
   
   # Ubuntu/Linux:
   curl -fsSL https://tailscale.com/install.sh | sh
   
   # Windows: Download from https://tailscale.com/download
   ```

4. **Connect Developer Machine**:
   ```bash
   tailscale up
   # Authenticate in browser when prompted
   
   # Verify connection
   tailscale ip
   ```

5. **Access Cluster Services**:
   ```bash
   # Direct access to cluster pods/services (no port-forward needed)
   kubectl get nodes
   kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
   curl http://airbyte-airbyte-server-svc:8000 -n airbyte
   ```

---

## 📦 Component Details & Databases

### Apache Airflow Configuration
- **Version**: 3.1.7
- **Executor**: KubernetesExecutor (dynamic pod scaling)
- **Docker Image**: custom-airflow:v1
- **API Server**: NodePort 32080
- **Persistent Storage**: 
  - PVC for DAGs and logs
  - Shared across scheduler and workers
- **Database**: Dedicated PostgreSQL instance (`airflow_db`)
  - Stores DAG metadata, task states, and execution history
  - Separate from Airbyte database for isolation
- **Providers Installed**:
  - apache-airflow-providers-airbyte (5.3.2)
  - apache-airflow-providers-google (19.5.0)
  - apache-airflow-providers-apache-spark (5.5.0)
- **Additional Tools**: Soda for data quality monitoring

### Airbyte Configuration
- **Edition**: Community
- **Deployment Mode**: OSS (Open Source)
- **Workload API**: Port 8007
- **Database**: Dedicated PostgreSQL instance (`airbyte_db`)
  - Stores connection configurations and sync history
  - Separate from Airflow database
  - StatefulSet for reliable metadata persistence
- **MinIO Container**: Object storage for state files
  - Stores job state and checkpoint data
  - Enables stateful ELT execution
  - Internal S3-compatible API
- **Workload Launcher**: Distributed job execution
- **Control Plane**: Local deployment (single-node cluster compatible)

### Apache Spark Configuration
- **Version**: 3.4.0
- **Executor Mode**: Cluster mode (Pod-per-job)
- **SparkApplication CRD**: Kubernetes native Spark job submission
- **Python Version**: Python 3
- **Key Dependencies**:
  - GCS Connector for Hadoop 3 (JAR dependency)
  - Spark BigQuery Connector (0.34.0)
- **Restart Policy**: OnFailure with exponential backoff
- **Networking**: DNS-based service discovery within cluster

---

## 🛠️ Deployment Prerequisites

### System Requirements
- Kubernetes cluster (1.19+)
- Helm 3.x
- kubectl CLI
- Docker (for building custom images)

### Cloud Requirements
- Google Cloud Project
- Service accounts with appropriate IAM roles:
  - `roles/bigquery.dataEditor`
  - `roles/storage.objectAdmin`
- GCS bucket for data staging
- BigQuery datasets configured

### Storage Requirements
- 10GB+ persistent storage for Airflow logs
- 50GB+ persistent storage for PostgreSQL
- GCS bucket for Spark job inputs/outputs

---

## � Deployment Guide (Ordered YAML Application)

Follow this **exact order** to deploy the complete platform. Each step depends on the previous one.

### Prerequisites Setup
```bash
# 1. Verify kubectl is configured and pointing to your cluster
kubectl cluster-info
kubectl get nodes

# 2. Verify Helm is installed
helm version

# 3. Set context to default or your preferred context
kubectl config use-context <your-context>
```

### Step-by-Step Deployment

#### Step 1: Create Namespaces & RBAC Foundation
```bash
# Create isolated namespaces for each component
kubectl create namespace airflow
kubectl create namespace airbyte
kubectl create namespace spark
kubectl create namespace spark-operator

# Apply RBAC policies FIRST (services need these permissions)
# ORDER MATTERS: Apply these in sequence
kubectl apply -f spark-operator-clusterrole.yaml        # Global Spark permissions
kubectl apply -f airflow-worker-role.yaml               # Airflow pod permissions
kubectl apply -f airflow-spark-role.yaml                # Airflow->Spark bridge
kubectl apply -f airflow-spark-rbac-and-rolebinding.yaml # Combined bindings
```

#### Step 2: Create Credentials & Secrets
```bash
# Create GCP credential secrets BEFORE deploying pods
# First, copy the example files and populate with your actual credentials:

cp keys/airbyte-sa-key.json.example keys/airbyte-sa-key.json
cp keys/spark-etl-key.json.example keys/spark-etl-key.json
cp keys/HMAC.txt.example keys/HMAC.txt

# Edit files with your actual GCP credentials:
# - airbyte-sa-key.json: GCP service account JSON key
# - spark-etl-key.json: GCP service account JSON key (can be same as above)
# - HMAC.txt: GCS HMAC access/secret credentials (two lines: access_key, secret_key)

# Create Kubernetes secrets from your populated files
kubectl create secret generic gcp-airflow-keys \
  --from-file=airbyte-sa.json=keys/airbyte-sa-key.json \
  --from-file=spark-etl.json=keys/spark-etl-key.json \
  -n airflow

kubectl create secret generic gcp-airbyte-keys \
  --from-file=sa-key.json=keys/airbyte-sa-key.json \
  -n airbyte

kubectl create secret generic gcp-spark-keys \
  --from-file=sa-key.json=keys/spark-etl-key.json \
  --from-file=hmac.txt=keys/HMAC.txt \
  -n spark
```

#### Step 3: Setup Persistent Storage
```bash
# Create persistent volumes for logging and databases
# Must be applied BEFORE any StatefulSets or applications
kubectl apply -f airflow-logs-pv.yaml
kubectl apply -f airflow-logs-pvc.yaml

# Verify PVCs are bound
kubectl get pvc -n airflow
```

#### Step 4: Deploy Spark Operator (Foundation Service)
```bash
# Spark Operator must be deployed first as other services depend on SparkApplication CRD
# Add Helm repo if not already added
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Install Spark Operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --set serviceAccount.create=true

# Wait for Spark Operator to be ready
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=spark-operator \
  -n spark-operator \
  --timeout=300s
```

#### Step 5: Deploy PostgreSQL Database Layer
```bash
# Deploy Airflow PostgreSQL (if not using managed database)
# Deploy Airbyte PostgreSQL (separate instance)
# Note: YAML files for PostgreSQL deployment should be created if not present
# For now, ensure PostgreSQL is accessible:

# Option A: Using managed databases (recommended for production)
# - Create CloudSQL instances in GCP
# - Configure connection strings in Helm values.yaml files

# Option B: Using in-cluster PostgreSQL (development)
# - Ensure PersistentVolumes are available (done in Step 3)
# - Apply your PostgreSQL Helm charts or StatefulSets

echo "Ensure both airflow_db and airbyte_db PostgreSQL instances are ready before proceeding"
kubectl get pvc,pv
```

#### Step 6: Deploy Airflow
```bash
# Add Airflow Helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install Airflow with custom values
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values airflow-values.yaml \
  --wait \
  --timeout 10m

# Verify Airflow is running
kubectl get pods -n airflow
kubectl get svc -n airflow

# Access Airflow UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
# Open: http://localhost:8080  
# From inside the cluster or expose an external IP so you can access the UI
```

#### Step 7: Deploy Airbyte
```bash
# Add Airbyte Helm repository
helm repo add airbyte https://airbytehq.github.io/helm-charts
helm repo update

# Install Airbyte with custom values
helm install airbyte airbyte/airbyte \
  --namespace airbyte \
  --values airbyte-values.yaml \
  --wait \
  --timeout 10m

# Verify Airbyte is running
kubectl get pods -n airbyte
kubectl get svc -n airbyte

# Access Airbyte UI
kubectl port-forward svc/airbyte-airbyte-webapp-svc 3000:80 -n airbyte
# Open: http://localhost:3000
```

#### Step 8: Deploy Service Networking (Optional)
```bash
# Apply additional networking configuration
kubectl apply -f airbyte-webapp-service.yaml        # Expose Airbyte webapp
kubectl apply -f workload-api-alias.yaml            # Internal DNS alias for workload API
```

#### Step 9: Deploy Spark Applications
```bash
# Deploy Spark jobs
# These will run as SparkApplication resources managed by the Spark Operator
# It will dynamically attach resources to driver/executer using the YAML configuration and 
# Kuberenetes Cluster Manager 
kubectl apply -f spark-apps/gcs-to-bq.yaml

# Monitor Spark job
kubectl describe sparkapplication gcs-to-bq -n spark
kubectl logs -l spark-role=driver -n spark --tail=100

# Example test job
kubectl apply -f spark-pi.yaml  # Simple Pi calculation for testing
```

#### Step 10: Setup Tailscale VPN (Secure Access)
```bash
# Install Tailscale in your cluster for secure developer access
# 1. Generate Tailscale auth key from https://login.tailscale.com/admin/keys
# 2. Create Tailscale Kubernetes secrets

kubectl create secret generic tailscale-auth \
  --from-literal=TS_AUTHKEY=<your-tailscale-auth-key> \
  -n tailscale

# Deploy Tailscale DaemonSet (create tailscale-daemonset.yaml first)
# This allows developers to connect via VPN instead of exposing services publicly
kubectl apply -f tailscale-daemonset.yaml
```

### Deployment Summary Table

| Order | Component | Namespace | YAML Files | Depends On |
|-------|-----------|-----------|-----------|-----------|
| 1 | Namespaces & RBAC | All | *-role.yaml, *-rolebinding.yaml | Nothing |
| 2 | GCP Secrets | All | (kubectl commands) | Step 1 |
| 3 | Persistent Storage | airflow | *-pv.yaml, *-pvc.yaml | Step 1 |
| 4 | Spark Operator | spark-operator | (Helm) | Step 1, 3 |
| 5 | PostgreSQL | Default/Cloud | (External or StatefulSet) | Step 3 |
| 6 | Airflow | airflow | Helm + airflow-values.yaml | Step 1,2,3,5 |
| 7 | Airbyte | airbyte | Helm + airbyte-values.yaml | Step 1,2,5 |
| 8 | Service Networking | airbyte | airbyte-webapp-service.yaml | Step 7 |
| 9 | Spark Jobs | spark | gcs-to-bq.yaml, spark-pi.yaml | Step 1,2,4,6,7 |
| 10 | Tailscale VPN | tailscale | tailscale-daemonset.yaml | Step 1 |

### Verification Checklist
```bash
# After deployment, verify everything is running:

# Check all namespaces exist
kubectl get namespace

# Check RBAC is configured
kubectl get clusterrole,clusterrolebinding | grep -E "spark|airflow"

# Check persistent volumes
kubectl get pv,pvc --all-namespaces

# Check pod status in all namespaces
kubectl get pods -n airflow
kubectl get pods -n airbyte
kubectl get pods -n spark
kubectl get pods -n spark-operator

# Check services and endpoints
kubectl get svc --all-namespaces
kubectl get endpoints --all-namespaces

# Check SparkApplications
kubectl get sparkapplications -n spark

# Check secrets
kubectl get secrets -n airflow
kubectl get secrets -n airbyte
kubectl get secrets -n spark
```

---

## 🔄 Data Flow

```
GCS Data Lake , PostgreSQL, Salesforce .. etc
    │
    ▼
┌─────────────────┐
│  Airbyte ELT    │ ◄─── Data Ingestion & Schema Discovery
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│       GCS       │ ◄─── Data Warehouse & Analytics
└─────────────────|
         │
         ▼
┌─────────────────┐
│  Airflow DAGs   │ ◄─── Workflow Orchestration & Scheduling
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Jobs     │ ◄─── Distributed Data Processing
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  GCS + BigQuery │ ◄─── Data Warehouse & Analytics
└─────────────────┘
```

---

## 📊 Example Workflow: GCS to BigQuery

The included `gcs-to-bq.yaml` Spark job demonstrates the complete pipeline:

1. **Input**: Parquet data from GCS bucket
2. **Processing**: Apache Spark cluster mode execution
3. **Output**: Loaded directly into BigQuery
4. **Monitoring**: Spark Operator manages job lifecycle

### Job Configuration
- **Project ID**: project_id
- **Input**: gs://<GCS bucket>/parquet_data/
- **Dataset**: BigQuery_database/table_name
- **Automatic Restart**: On failure

---

## 🔍 Monitoring & Management

### Airflow UI
Access the Airflow UI at `http://localhost:32080`

- DAG visualization and execution history
- Task logs and error tracking
- Metrics and performance insights

### Kubernetes Dashboard
```bash
# View Airflow pods
kubectl get pods -n airflow

# View Airbyte pods
kubectl get pods -n airbyte

# View Spark jobs
kubectl get sparkapplications -n spark

# View logs
kubectl logs <pod-name> -n <namespace>
```

### Health Checks
```bash
# Check Airflow API
curl http://localhost:32080/api/v1/health

# Check Airbyte server
curl http://airbyte-airbyte-server-svc:8000/health

# Monitor Spark jobs
kubectl describe sparkapplication gcs-to-bq -n spark
```

---

## 🛠️ Troubleshooting

### Common Issues

**Airflow Pods Not Starting**
- Check PostgreSQL connectivity
- Verify persistent volume availability
- Review Airflow logs: `kubectl logs <airflow-scheduler> -n airflow`

**Spark Job Failures**
- Verify GCP service account credentials
- Check GCS bucket permissions
- Review job status: `kubectl describe sparkapplication gcs-to-bq -n spark`

**Airbyte Connection Issues**
- Verify PostgreSQL is running
- Check workload-api-server connectivity
- Review Airbyte logs: `kubectl logs <airbyte-pod> -n airbyte`

---

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [Airbyte Documentation](https://docs.airbyte.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Google Cloud Documentation](https://cloud.google.com/docs)





