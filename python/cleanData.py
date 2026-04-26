import pandas as pd
import os
import glob

# folders
INPUT_FOLDER = "raw_data"
OUTPUT_FOLDER = "MapVis/python/cleaned_data"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Clean Pollutant
def normalize_pollutant(name):
    name = str(name).lower()

    if "nitrogen dioxide" in name or "no2" in name:
        return "NO2"
    elif "pm2.5" in name:
        return "PM2.5"
    elif "pm10" in name:
        return "PM10"
    elif "ozone" in name:
        return "Ozone"
    elif "sulfur dioxide" in name or "so2" in name:
        return "SO2"
    elif "carbon monoxide" in name or "co" in name:
        return "CO"
    elif "lead" in name:
        return "Pb"
    else:
        return "Other"

# Cleaning Function
def clean_dataset(filepath):

    df = pd.read_csv(filepath)

    # column names
    df.columns = df.columns.str.strip()

    # text cleaning
    text_cols = ["State Name", "County Name", "City Name", "Parameter Name"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # zero padding on state and county codes
    if "State Code" in df.columns:
        df["State Code"] = df["State Code"].astype(str).str.zfill(2)

    if "County Code" in df.columns:
        df["County Code"] = df["County Code"].astype(str).str.zfill(3)

    # missing values
    df.replace(["N/A", "", " "], pd.NA, inplace=True)

    # save required rows and drop incomplete rows
    required_cols = ["State Code", "Parameter Name", "Arithmetic Mean"]
    df = df.dropna(subset=[col for col in required_cols if col in df.columns])

    # numeric columns
    numeric_cols = ["Arithmetic Mean", "Latitude", "Longitude"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # remove bad values
    if "Arithmetic Mean" in df.columns:
        df = df.dropna(subset=["Arithmetic Mean"])

    # pollutant names
    df["pollutant_clean"] = df["Parameter Name"].apply(normalize_pollutant)

    # duplicates
    df = df.drop_duplicates()

    return df

# Process All CSV Files
files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))

for file in files:
    print("Hit.")
    cleaned_df = clean_dataset(file)

    filename = os.path.basename(file)
    output_file = os.path.join(OUTPUT_FOLDER, filename)

    cleaned_df.to_csv(output_file, index=False)

    print("Cleaned:", filename)

print("Files cleaned.")