import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_and_convert_files(taxi_type):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    # Define year range based on taxi type
    if taxi_type == "fhv":
        years = [2019]  # Only 2019 for FHV
    else:
        years = [2019, 2020]

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            if taxi_type == "fhv":
                file_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz"
            else:
                file_url = f"{BASE_URL}/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            print(f"Downloading {csv_gz_filename}...")
            response = requests.get(file_url, stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            with duckdb.connect() as con:
                con.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                    TO '{parquet_filepath}' (FORMAT PARQUET)
                """)

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')

if __name__ == "__main__":
    # Update .gitignore to exclude data directory
    update_gitignore()

    for taxi_type in ["yellow", "green", "fhv"]:
        download_and_convert_files(taxi_type)

    with duckdb.connect("taxi_rides_ny.duckdb") as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS prod")

        # Load yellow and green data
        for taxi_type in ["yellow", "green"]:
            con.execute(f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
            """)
        
        # Load FHV data
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.fhv_tripdata AS
            SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
        """)

    con.close()