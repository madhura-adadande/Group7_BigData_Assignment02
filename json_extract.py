import os
import json
import boto3
import pandas as pd
import concurrent.futures
from dotenv import load_dotenv

# ✅ Load Environment Variables
load_dotenv()

# ✅ AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# ✅ S3 Client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# ✅ Dynamic Quarter Selection
QUARTER = os.getenv("Quarter")
SEC_FOLDER = f"Raw_Data/{QUARTER}/"  # Source Folder in S3
JSON_S3_FOLDER = f"Json_Data/{QUARTER}/"  # Target Folder in S3

# ✅ Local Paths
LOCAL_FOLDER = "extraction"
LOCAL_TICKER_FILE = "/Users/vemana/Desktop/Financial_Data_Integration_and_Transformation/extraction/ticker.txt"  # Use absolute path
LOCAL_FILES = ["sub.txt", "num.txt", "pre.txt", "tag.txt"]


def check_ticker_file():
    """Ensure `ticker.txt` exists before processing."""
    if not os.path.exists(LOCAL_TICKER_FILE):
        print(f"❌ Error: `{LOCAL_TICKER_FILE}` not found! Make sure `ticker.txt` is in the correct path.")
        exit(1)


def download_sec_files():
    """Download SEC files from S3 to local extraction folder."""
    os.makedirs(LOCAL_FOLDER, exist_ok=True)

    for file in LOCAL_FILES:
        s3_key = SEC_FOLDER + file
        local_path = os.path.join(LOCAL_FOLDER, file)

        try:
            print(f"📥 Downloading {file} from S3...")
            s3.download_file(AWS_BUCKET_NAME, s3_key, local_path)
        except Exception as e:
            print(f"❌ Failed to download {file}: {e}")


def load_ticker_data():
    """Load `ticker.txt` into a Pandas DataFrame."""
    check_ticker_file()

    try:
        df = pd.read_csv(LOCAL_TICKER_FILE, delimiter="\t", names=["ticker", "cik"], dtype=str)
        df["cik"] = df["cik"].str.zfill(10)  # Pad CIK with leading zeros
        print(f"✅ Loaded {len(df)} tickers.")
        return df
    except Exception as e:
        print(f"❌ Error loading ticker.txt: {e}")
        return None


def process_company(cik, group, ticker_df, num_df, pre_df, tag_df):
    """Process an individual company and directly upload JSON to S3 (No local storage)."""
    ticker = ticker_df[ticker_df["cik"] == cik]["ticker"].values[0] if cik in ticker_df["cik"].values else f"Unknown_{cik}"

    # 🔹 Get Related Data
    adsh_list = group["adsh"].unique()
    num_data = num_df[num_df["adsh"].isin(adsh_list)]
    pre_data = pre_df[pre_df["adsh"].isin(adsh_list)]
    tag_data = tag_df[tag_df["tag"].isin(num_data["tag"].unique())]

    # 🔹 Create JSON Structure
    company_json = {
        "cik": cik,
        "ticker": ticker,
        "company_name": group.iloc[0]["name"],
        "sic": group.iloc[0]["sic"],
        "sub": group.to_dict(orient="records"),
        "num": num_data.to_dict(orient="records"),
        "pre": pre_data.to_dict(orient="records"),
        "tag": tag_data.to_dict(orient="records"),
    }

    # 🔹 Convert JSON to String and Upload Directly to S3
    json_data = json.dumps(company_json, indent=4)
    s3_key = f"{JSON_S3_FOLDER}{ticker}.json"  # ✅ Target path in S3

    try:
        print(f"📤 Uploading JSON directly to S3: {s3_key}...")
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key=s3_key, Body=json_data, ContentType="application/json")
        print(f"✅ Successfully uploaded {ticker}.json to S3!")
    except Exception as e:
        print(f"❌ Failed to upload {ticker}.json: {e}")


def transform_and_upload_json():
    """Process each company in parallel, reducing memory usage & speeding up execution."""
    print("🚀 Transforming SEC Data to JSON...")

    # ✅ Load SEC Files
    sub_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "sub.txt"), delimiter="\t", dtype=str)
    num_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "num.txt"), delimiter="\t", dtype=str)
    pre_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "pre.txt"), delimiter="\t", dtype=str)
    tag_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "tag.txt"), delimiter="\t", dtype=str)

    # ✅ Load Ticker Data
    ticker_df = load_ticker_data()
    if ticker_df is None:
        print("❌ Ticker data could not be loaded. Exiting.")
        exit(1)

    # ✅ Merge Ticker Data with `sub.txt`
    sub_df["cik"] = sub_df["cik"].astype(str).str.zfill(10)
    merged_df = sub_df.merge(ticker_df, on="cik", how="left")

    # ✅ Process Companies in Parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_company, cik, group, ticker_df, num_df, pre_df, tag_df)
            for cik, group in merged_df.groupby("cik")
        ]
        concurrent.futures.wait(futures)  # Wait for all threads to complete


if __name__ == "__main__":
    print(f"🚀 Running SEC JSON Pipeline for {QUARTER}...")

    # ✅ Step 1: Download SEC Files from S3
    download_sec_files()

    # ✅ Step 2: Transform Data to JSON (Per Company) & Upload to S3
    transform_and_upload_json()

    print("🎉 JSON Transformation Pipeline Completed Successfully!")
