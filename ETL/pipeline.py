import os
import zipfile
import requests
import pandas as pd
import sqlite3
from io import BytesIO

# Define directories
data_directory = "../data"
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# North American countries list
north_american_countries = [
    "Canada", "United States", "Mexico", "Bermuda", "Bahamas, The",
    "Barbados", "Cuba", "Haiti", "Dominican Republic", "Jamaica",
    "Trinidad and Tobago", "Saint Kitts and Nevis", "Antigua and Barbuda",
    "Saint Lucia", "Saint Vincent and the Grenadines", "Grenada", "Belize",
    "Panama", "Costa Rica", "El Salvador", "Honduras", "Nicaragua",
    "Guatemala"
]

# URLs for GDP and education expenditure data from Worldbank
url_gdp_zip = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
url_edu_zip = "https://api.worldbank.org/v2/en/indicator/SE.XPD.TOTL.GD.ZS?downloadformat=csv"

# Function to download and extract ZIP files
def download_and_extract_zip(url, output_dir):
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(output_dir)
            print(f"Extracted files to {output_dir}")
            csv_files = [f for f in z.namelist() if f.endswith(".csv") and "Metadata" not in f]
            return [os.path.join(output_dir, f) for f in csv_files]
    else:
        print(f"Failed to download data from {url}.")
        return []

# Process CSV file to filtering and reshaping
def clean_and_reshape_data(file_path, countries, years):
    df = pd.read_csv(file_path, skiprows=4)
    # Filter countries and select years
    df_filtered = df[df["Country Name"].isin(countries)][["Country Name", "Country Code"] + years]
    # Reshape from wide to long format
    df_long = df_filtered.melt(
        id_vars=["Country Name", "Country Code"],
        var_name="Year",
        value_name="Value"
    )
    return df_long

# Save cleaned data to SQLite
def export_to_sqlite(df, table_name, db_path):
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Saved table '{table_name}' to SQLite database at {db_path}.")

# Download, process, and save GDP data
gdp_files = download_and_extract_zip(url_gdp_zip, data_directory)
edu_files = download_and_extract_zip(url_edu_zip, data_directory)

# Filter years
years = [str(year) for year in range(2016, 2023)]

# Clean and reshape data
if gdp_files:
    gdp_cleaned = clean_and_reshape_data(gdp_files[0], north_american_countries, years)
    gdp_cleaned.to_csv(os.path.join(data_directory, "gdp_cleaned.csv"), index=False)
    print("Cleaned GDP data saved as CSV.")
    export_to_sqlite(gdp_cleaned, "gdp_data", os.path.join(data_directory, "data_cleaned.db"))

if edu_files:
    edu_cleaned = clean_and_reshape_data(edu_files[0], north_american_countries, years)
    edu_cleaned.to_csv(os.path.join(data_directory, "edu_cleaned.csv"), index=False)
    print("Cleaned Education Expenditure data saved as CSV.")
    export_to_sqlite(edu_cleaned, "education_data", os.path.join(data_directory, "data_cleaned.db"))
