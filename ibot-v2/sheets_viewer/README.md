# iBot v2 BigQuery Viewer for Google Sheets

View BigQuery data from iBot v2 imports directly in Google Sheets.

## Setup Instructions

### 1. Create a new Google Sheet
Go to [sheets.google.com](https://sheets.google.com) and create a new spreadsheet.

### 2. Open Apps Script Editor
- Go to **Extensions** → **Apps Script**
- This opens the script editor

### 3. Add the Code
- Delete any existing code in `Code.gs`
- Copy the contents of `Code.gs` from this folder
- Paste into the Apps Script editor

### 4. Enable BigQuery API
- In Apps Script editor, click **Services** (+ icon on left sidebar)
- Scroll down and find **BigQuery API**
- Click **Add**

### 5. Save and Authorize
- Click **Save** (Ctrl+S)
- Close the Apps Script editor
- Refresh the Google Sheet
- You'll see a new menu: **iBot v2**
- Click any menu item to authorize the script

### 6. Grant Permissions
When prompted, click **Advanced** → **Go to iBot v2 Viewer (unsafe)** → **Allow**

This grants the script permission to:
- Access BigQuery data
- Modify the spreadsheet

## Usage

### Quick Load
Use the **iBot v2** menu to quickly load data:
- **Load BA Produk LAZ** - Load Lazada product data
- **Load BA Produk SHO** - Load Shopee product data
- etc.

### Custom Query
**iBot v2** → **Custom Query...** to filter by:
- Category
- Brand code
- Date
- Row limit

### Refresh Data
**iBot v2** → **Refresh Current Sheet** to reload the current data sheet

## Available Categories

| Category | BigQuery Table |
|----------|----------------|
| BA Produk LAZ | ba_produk_laz |
| BA Produk SHO | ba_produk_sho |
| BA Produk TIK | ba_produk_tik |
| BA Dash LAZ | ba_dash_laz |
| BA Dash SHO | ba_dash_sho |
| BA Dash TIK | ba_dash_tik |
| BA Dash TOK | ba_dash_tok |
| Informasi Dasar SHO | informasi_dasar_sho |
| Informasi Dikirim SHO | informasi_dikirim_sho |
| Informasi Media SHO | informasi_media_sho |
| Informasi Penjualan SHO | informasi_penjualan_sho |
| Export SKU LAZ | export_sku_laz |
| Export SKU TIK | export_sku_tik |
| Demografis BSL | demografis_bsl |
| Proyeksi Stok BSL | proyeksi_stok_bsl |

## Troubleshooting

### "BigQuery API not enabled"
Make sure you added the BigQuery service in Apps Script (Step 4)

### "Access Denied"
The Google account needs BigQuery access to `fbi-dev-484410` project

### "No data found"
The table might be empty. Try uploading data first via GCS.
