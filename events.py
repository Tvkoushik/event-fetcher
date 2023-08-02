# The code is importing necessary libraries and modules for web scraping and data manipulation.
from bs4 import BeautifulSoup
import pandas as pd
import time
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import sys
from io import StringIO

# python3.10 events.py datalake-567267412705 external-data/events_data/ datalake-567267412705 external-data/major_events.csv

# total arguments
n = len(sys.argv)
print("Total arguments passed:", n)

# S3 bucket name and CSV file paths
BUCKET_NAME = sys.argv[1]
print(f"Bucket name : {BUCKET_NAME}")

PATH = sys.argv[2]
print(f"S3 File Path : {PATH}")

# MAJOR_EVENTS_S3_BUCKET_NAME = sys.argv[3]
# print(f"Bucket Name of Major events file path : {MAJOR_EVENTS_S3_BUCKET_NAME}")

# MAJOR_EVENTS_CSV_FILE_PATH = sys.argv[4]
# print(f"Major events S3 file path : {MAJOR_EVENTS_CSV_FILE_PATH}")

# create a Firefox Options object
print("Creating Firefox Options Object")
options = Options()
options.add_argument("-headless")  # run Firefox in headless mode

# create a new Firefox browser instance with the options
driver = webdriver.Firefox(options=options)


# Function to upload file to S3
def upload_to_s3(bucket, path):
    s3 = boto3.client("s3")

    # Get the current date
    date = datetime.now().strftime("%Y-%m-%d")

    file_name = "events.csv"

    # Generate the S3 file name
    s3_file_name = (
        f"{path}date={date}/events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    try:
        s3.upload_file(file_name, bucket, s3_file_name)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


# Function to convert dates
def convert_dates(date_range):
    dates = date_range.split(" - ")
    start_date_str, end_date_str = dates if len(dates) > 1 else (dates[0], dates[0])

    year = str(datetime.now().year)

    # append year only when end date is not 'ongoing'
    if "ongoing" not in end_date_str.lower():
        year = end_date_str.split(", ")[-1]

    if len(start_date_str.split(" ")) == 2:
        start_date_str += ", " + year

    start_date = datetime.strptime(start_date_str, "%b %d, %Y")

    if "ongoing" in end_date_str.lower():
        end_date = datetime.now()  # set to today's date
    else:
        end_date = datetime.strptime(end_date_str, "%b %d, %Y")

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


# Begin main script
try:
    # navigate to the webpage
    print("Navigating Webpage")
    driver.get("https://www.sandiego.org/explore/events.aspx")

    # define initial wait for the presence of one event
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.result")))

    # find the Load More button
    print("Searching for Load More Button")
    load_more_button = driver.find_element(By.CSS_SELECTOR, "button.load-more")

    # click the Load More button until all events are loaded
    while True:
        try:
            load_more_button.click()
            time.sleep(2)  # pause to load content
        except Exception:
            break

    # get the current HTML of the page
    print("Downloading the HTML Page")
    html = driver.page_source

    # save the HTML to a file
    with open("events.html", "w") as f:
        f.write(html)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # close the browser
    print("Closing the Browser")
    driver.quit()

# Load the HTML file
with open("events.html") as f:
    web_code = f.read()

# Parse the HTML with Beautiful Soup
soup = BeautifulSoup(web_code, "html.parser")

# Find all the event sections
print("Scraping all Events")
event_sections = soup.find_all("section", {"class": "result"})

# Prepare empty lists to store the event names and dates
event_names = []
event_dates = []
start_dates = []
end_dates = []

print("Fetching event names and dates formatted to YYYY-MM-DD")
# Loop over each event section and extract the event names and dates
for section in event_sections:
    event_name = section.find("h1", {"class": "result__title"}).find("a")
    event_date = section.find("div", {"class": "result__dates"})

    if event_name and event_date:
        event_names.append(event_name.text.strip().split(". ")[1])
        start_date, end_date = convert_dates(event_date.text.strip())
        start_dates.append(start_date)
        end_dates.append(end_date)

print("Creating a dataframe of events")
# Create a DataFrame from the event names and dates
df = pd.DataFrame(
    {
        "event_name": event_names,
        "start_date": start_dates,
        "end_date": end_dates,
    }
)

df["record_load_timestamp"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

df.to_csv("events.csv", index=False)

# Experimental Code which uses Major event as a lookup to create the major_event flag column(Y/N)
# # S3 client
# s3 = boto3.client("s3")

# try:
#     print("Reading the Master File from S3")
#     # Get the CSV file from S3
#     obj = s3.get_object(
#         Bucket=MAJOR_EVENTS_S3_BUCKET_NAME, Key=MAJOR_EVENTS_CSV_FILE_PATH
#     )
#     # Read the CSV file into a DataFrame
#     major_events_df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
# except Exception as e:
#     print(f"An error occurred when reading the file from S3: {e}")
#     raise

# try:
#     # Perform a left join on the DataFrames
#     print("Merging DataFrames")
#     merged_df = df.merge(
#         major_events_df, how="left", left_on="event_name", right_on="mapping_event"
#     )

#     # Add a new column to indicate if the event is a major one
#     print("Adding new column")
#     merged_df["is_major_event"] = merged_df["mapping_event"].apply(
#         lambda x: "Y" if pd.notna(x) else "N"
#     )

#     merged_df = merged_df.drop(columns=["major_events", "mapping_event"])
# except Exception as e:
#     print(f"An error occurred when merging DataFrames or adding new column: {e}")
#     raise

# merged_df["record_load_timestamp"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

# # Save the merged DataFrame to the CSV file
# merged_df.to_csv("events.csv", index=False)

# Upload the file to S3
upload_to_s3(BUCKET_NAME, PATH)
