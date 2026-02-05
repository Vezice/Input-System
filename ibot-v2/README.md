# iBot v2

Cloud Functions-based file processor for importing Excel/CSV data to BigQuery.

## Architecture

```
GCS Bucket (ibot-v2-imports)       Cloud Function               BigQuery
┌─────────────────────┐           ┌──────────────┐           ┌──────────────┐
│ BA Dash LAZ/        │──trigger─▶│ ibot-v2-import│──stream──▶│  ibot_data   │
│   GS file.xlsx      │           │   (Python)    │           │  .ba_dash_laz│
└─────────────────────┘           └───────┬──────┘           └──────────────┘
                                         │
                                         ▼
                                    Slack Webhook
```

## GCS Folder Structure

```
gs://ibot-v2-imports/
├── {CATEGORY}/              # Files waiting to be processed
│   └── {BRAND} filename.xlsx
├── archive/                 # Successfully processed
│   └── {CATEGORY}/...
└── failed/                  # Failed imports
    └── {CATEGORY}/...
```

**Categories**: BA Dash LAZ/SHO/TIK/TOK, BA Produk LAZ/SHO/TIK, Informasi Dasar/Dikirim/Media/Penjualan SHO, Export SKU LAZ/TIK, Demografis BSL, Proyeksi Stok BSL

## File Naming Convention

```
{BRAND_CODE} {original_filename}.xlsx
```

Example: `GS 2024-01-15_product_data.xlsx`
- Brand code: `GS`
- Parsed from filename (text before first space)

## Initial Setup

### 1. Create GCS Bucket

```bash
gsutil mb -p fbi-dev-484410 -l asia-southeast2 gs://ibot-v2-imports/
```

### 2. Create BigQuery Tables

```bash
cd ibot-v2
pip install google-cloud-bigquery
python setup_bigquery.py

# Or dry-run to see what would be created:
python setup_bigquery.py --dry-run
```

### 3. Deploy Cloud Functions

```bash
./deploy.sh
```

## Deployment

### Manual Deployment

```bash
cd ibot-v2
./deploy.sh
```

### Cloud Build (CI/CD)

```bash
gcloud builds submit --config=cloudbuild.yaml
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOOGLE_CLOUD_PROJECT` | fbi-dev-484410 | GCP Project ID |
| `IMPORT_BUCKET` | ibot-v2-imports | GCS bucket name |
| `BIGQUERY_DATASET` | ibot_v2_data | BigQuery dataset |
| `SLACK_ENABLED` | true | Enable Slack notifications |
| `ADMIN_SHEET_ID` | (hardcoded) | Google Sheets ID for config |

### Admin Sheet Configuration

Config is loaded dynamically from the Admin Sheet:
- **List** sheet: Slack webhook URLs per category
- **Type Validation** sheet: Header row, data row, required headers
- **Unique Column** sheet: Column aliases

## API Endpoints

### Health Check
```bash
curl https://asia-southeast2-fbi-dev-484410.cloudfunctions.net/ibot-v2-http/health
```

### Manual Processing
```bash
curl -X POST https://asia-southeast2-fbi-dev-484410.cloudfunctions.net/ibot-v2-http/process \
  -H "Content-Type: application/json" \
  -d '{"path": "BA Produk LAZ/GS data.xlsx"}'
```

## Testing

Upload a test file:
```bash
gsutil cp test.xlsx gs://ibot-v2-imports/BA\ Produk\ LAZ/TEST\ test.xlsx
```

Check logs:
```bash
gcloud functions logs read ibot-v2-import --region=asia-southeast2 --limit=50
```

## BigQuery Schema

All tables include metadata columns:
- `_import_id` (STRING): Unique import identifier
- `_brand_code` (STRING): Brand code from filename
- `_source_file` (STRING): Original filename
- `_import_timestamp` (TIMESTAMP): When imported
- `_row_number` (INTEGER): Row number in source file

All data columns are STRING type. Type casting is done in downstream dbt/queries.

## Files

| File | Description |
|------|-------------|
| `main.py` | Cloud Function entry points |
| `config.py` | Admin Sheet config loader |
| `parser.py` | Excel/CSV parsing |
| `bigquery_loader.py` | BigQuery streaming insert |
| `slack_notifier.py` | Slack notifications |
| `utils/logger.py` | Structured logging |
| `utils/gcs_utils.py` | GCS operations |
| `setup_bigquery.py` | BigQuery table setup script |
| `deploy.sh` | Manual deployment script |
| `cloudbuild.yaml` | CI/CD configuration |

## Viewing Data in Google Sheets

Use the included Apps Script to view BigQuery data directly in Google Sheets.

See `sheets_viewer/README.md` for setup instructions.

Features:
- Load any category with one click
- Filter by brand, date
- Auto-formatted tables with frozen headers

## Migration from v1 (Apps Script)

1. Deploy v2 and test with manual uploads
2. Run v1 and v2 in parallel, compare outputs
3. Add Slack command to move files from Drive to GCS
4. Disable v1 triggers when confident
