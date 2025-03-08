import os
import time
import requests
import boto3
import zipfile
import logging
from io import BytesIO
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# SEC Website Configuration
SEC_URL = "https://www.sec.gov/dera/data/financial-statement-data-sets.html"
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL")
SEC_USER_AGENT = (
    f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    f"AppleWebKit/537.36 (KHTML, like Gecko) "
    f"Chrome/120.0.0.0 Safari/537.36 (Contact: {CONTACT_EMAIL})"
)

# Set up Selenium Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument(f"user-agent={SEC_USER_AGENT}")

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def get_sec_dataset_links():
    """Scrape SEC dataset links (ZIP files) using Selenium."""
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        driver.get(SEC_URL)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
        
        links = [
            elem.get_attribute("href")
            for elem in driver.find_elements(By.TAG_NAME, "a")
            if elem.get_attribute("href") and elem.get_attribute("href").endswith(".zip")
        ]
        return links
    except Exception as e:
        logging.error(f"Error fetching SEC data: {e}")
        return []
    finally:
        driver.quit()

def download_extract_and_upload_to_s3(url):
    """Download, unzip, and upload extracted files to S3 inside Raw_Data folder."""
    filename = url.split("/")[-1]
    folder_name = filename.replace(".zip", "")
    s3_folder = f"Raw_Data/{folder_name}"  # Ensuring files go inside Raw_Data
    
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "From": CONTACT_EMAIL,
        "Referer": "https://www.sec.gov/",
        "Accept": "application/zip, application/octet-stream, application/x-zip-compressed, multipart/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
    }
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            for file_name in zip_file.namelist():
                with zip_file.open(file_name) as file_data:
                    s3_key = f"{s3_folder}/{file_name}"  # Store in Raw_Data
                    logging.info(f"Uploading {s3_key} to S3...")
                    s3_client.upload_fileobj(file_data, AWS_BUCKET_NAME, s3_key)
                    logging.info(f"Uploaded {s3_key} successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")
    except Exception as e:
        logging.error(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    dataset_links = get_sec_dataset_links()
    
    if not dataset_links:
        logging.info("No dataset links found.")
    
    for link in dataset_links:
        time.sleep(2)
        download_extract_and_upload_to_s3(link)
