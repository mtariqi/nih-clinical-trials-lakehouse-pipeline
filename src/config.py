import os
from dotenv import load_dotenv

load_dotenv()

# ── NIH API ──────────────────────────────────────────────────────────────────
API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
API_PAGE_SIZE = 100
API_FIELDS = [
    "NCTId", "BriefTitle", "OfficialTitle", "OverallStatus",
    "StartDate", "CompletionDate", "LastUpdatePostDate",
    "Phase", "StudyType", "EnrollmentCount",
    "Condition", "Keyword", "LeadSponsorName", "LeadSponsorClass",
    "LocationCountry", "PrimaryOutcomeMeasure",
    "MinimumAge", "MaximumAge", "Sex", "HealthyVolunteers",
]

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "clinical-trials-updates"
KAFKA_GROUP_ID = "nih-pipeline-consumer"

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET", "nih-trials-raw")
S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "nih-trials-bronze")
S3_SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "nih-trials-silver")
S3_GOLD_BUCKET = os.getenv("S3_GOLD_BUCKET", "nih-trials-gold")

# ── Redshift ──────────────────────────────────────────────────────────────────
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "localhost")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "nih_trials")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "admin")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")
REDSHIFT_SCHEMA = "public"

# ── MLflow ────────────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = "nih-trial-outcome-prediction"

# ── Local paths ───────────────────────────────────────────────────────────────
DATA_RAW_DIR = "data/raw"
DATA_PROCESSED_DIR = "data/processed"

# ── Pipeline watermark ────────────────────────────────────────────────────────
WATERMARK_FILE = "data/raw/.last_update_date"

# ── Allowed values ────────────────────────────────────────────────────────────
VALID_STATUSES = [
    "Recruiting", "Completed", "Terminated", "Withdrawn",
    "Active, not recruiting", "Not yet recruiting",
    "Enrolling by invitation", "Suspended", "Unknown status",
]
VALID_PHASES = [
    "Phase 1", "Phase 2", "Phase 3", "Phase 4",
    "Phase 1/Phase 2", "Phase 2/Phase 3", "Early Phase 1", "N/A",
]
