#!/bin/bash
# ----------------------------------------
# Download FracFocus ZIP
# Merge all *FracFocusRegistry*.csv into one
# Truncate and load into RDS table: frac_data
# ----------------------------------------

set -euo pipefail

# ---- Setup logging ----
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/run_$(date +%F_%H-%M-%S).log"

# Redirect all stdout/stderr to the log file
exec > >(tee -a "$LOG_FILE") 2>&1

# Email on failure
EMAIL="atheeralattar@gmail.com"
trap 'echo "Script failed. Sending error email..."; tail -n 50 "$LOG_FILE" | mail -s "FracFocus Load Failed" "$EMAIL"' ERR

# ---- CONFIG ----
URL="https://www.fracfocusdata.org/digitaldownload/FracFocusCSV.zip"
ZIP_FILE="FracFocusCSV.zip"
EXTRACT_DIR="./fracfocus_data"
PROD_FILE="merged_fracfocus.csv"
# test file is test_fracfocus.csv"
DB_HOST="database-1.cvww2uskuvag.us-west-2.rds.amazonaws.com"
DB_PORT="5432"
DB_NAME="fracFocus"
DB_USER="postgres"
DB_PASSWORD="Elite2021__"
TABLE_NAME="frac_data"

# ---- Download ZIP ----
echo -e "📥 Downloading FracFocus ZIP...\n"
wget -O "$ZIP_FILE" "$URL"

# ---- Unzip ----
echo -e "📦 Extracting ZIP...\n "
mkdir -p "$EXTRACT_DIR"
unzip -o "$ZIP_FILE" -d "$EXTRACT_DIR"

# ---- Merge *FracFocusRegistry*.csv files into one ----
echo -e "🧩 Merging FracFocusRegistry CSVs...\n"
FIRST=1
> "$PROD_FILE"  # Clear output file

for file in "$EXTRACT_DIR"/*FracFocusRegistry*.csv; do
  if [ "$FIRST" -eq 1 ]; then
    cat "$file" >> "$PROD_FILE"
    FIRST=0
  else
    tail -n +2 "$file" >> "$PROD_FILE"  # Skip header
  fi
done

# # ---- Export password ----
export PGPASSWORD="$DB_PASSWORD"

# ---- Create frac_data table with all TEXT columns ----
echo -e "🧱 Creating table $TABLE_NAME with TEXT columns...\n"
header_line=$(head -n 1 "$PROD_FILE")
IFS=',' read -ra cols <<< "$(echo -e "$header_line" | sed 's/"//g')"
col_defs=$(printf '"%s" TEXT, ' "${cols[@]}")
col_defs="${col_defs%, }"

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
DROP TABLE IF EXISTS "$TABLE_NAME";
CREATE TABLE "$TABLE_NAME" ($col_defs);
EOF

# ---- Clean up empty strings (convert to Postgres NULLs) ----
echo -e "🧹 Converting empty strings to NULLs...\n"
sed -i 's/""/\\N/g' "$PROD_FILE"
sed -i 's/,,/,\\N,/g' "$PROD_FILE"
sed -i 's/,$/,\\N/' "$PROD_FILE"

# ---- Load into table ----
echo -e "📤 Loading merged CSV into $TABLE_NAME..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy $TABLE_NAME FROM '$PROD_FILE' WITH CSV HEADER NULL '\\N';"

# ---- Fixing columns data types ----
echo -e "⏳ Fixing columns data types...\n"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
ALTER TABLE "$TABLE_NAME"
  ALTER COLUMN "JobStartDate" TYPE TIMESTAMP USING NULLIF("JobStartDate", '')::TIMESTAMP,
  ALTER COLUMN "JobEndDate" TYPE TIMESTAMP USING NULLIF("JobEndDate", '')::TIMESTAMP,
  ALTER COLUMN "Latitude" TYPE FLOAT USING NULLIF("Latitude", '')::FLOAT,
  ALTER COLUMN "Longitude" TYPE FLOAT USING NULLIF("Longitude", '')::FLOAT,
  ALTER COLUMN "TVD" TYPE FLOAT USING NULLIF("TVD", '')::FLOAT,
  ALTER COLUMN "TotalBaseWaterVolume" TYPE FLOAT USING NULLIF("TotalBaseWaterVolume", '')::FLOAT,
  ALTER COLUMN "TotalBaseNonWaterVolume" TYPE FLOAT USING NULLIF("TotalBaseNonWaterVolume", '')::FLOAT,
  ALTER COLUMN "PercentHighAdditive" TYPE FLOAT USING NULLIF("PercentHighAdditive", '')::FLOAT,
  ALTER COLUMN "PercentHFJob" TYPE FLOAT USING NULLIF("PercentHFJob", '')::FLOAT,
  ALTER COLUMN "MassIngredient" TYPE FLOAT USING NULLIF("MassIngredient", '')::FLOAT;
EOF

# ---- Clean up files ----
echo -e "🧼 Cleaning up...\n"
rm -f "$ZIP_FILE"
rm -rf "$EXTRACT_DIR"
rm -f "$PROD_FILE"

echo -e "✅ Done! FracFocus data loaded into $TABLE_NAME"


# Email on success
SUBJECT="✅ FracFocus load completed successfully"
tail -n 50 "$LOG_FILE" | mail -s "$SUBJECT" "$EMAIL"