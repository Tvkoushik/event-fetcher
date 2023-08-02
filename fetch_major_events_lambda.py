import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import boto3
from datetime import datetime
import os
from io import StringIO

base_url = 'https://www.sandiego.org'
BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_PATH = os.environ['BUCKET_PATH']

def fetch_data():
    # Fetch the HTML content
    html_text = requests.get('https://www.sandiego.org/articles/events/major-events-in-san-diego.aspx').text

    # Parse the content
    soup = BeautifulSoup(html_text,'html.parser')

    # Find the event names and links
    event_names = soup.find_all('h1',class_ = 'heading--small')
    event_links = soup.find_all('a',class_ = 'short-content__cta-link hoverwhite')

    # Extract the text and href attributes
    all_events = [event.text for event in event_names]
    all_links = [event['href'] for event in event_links]

    # Loop over the event links and fetch the dates
    event_dates = []
    for link in all_links:
        # Check if the link is a relative URL and, if so, convert it to an absolute URL
        if link.startswith('/'):
            event_dates.append(None)  # Append None for invalid links
            continue  # Skip the rest of the loop for this link

        # Use requests to fetch the subpage content
        subpage = requests.get(link)
        
        # Parse the subpage with BeautifulSoup
        subpage_soup = BeautifulSoup(subpage.content, 'html.parser')
        
        # Find the date
        date_div = subpage_soup.find_all('div', {'class': 'extra-block'})
        date = None
        for block in date_div:
            h3_tag = block.find('h3', {'class': 'tag'})
            if h3_tag and h3_tag.text.strip() == 'Date & Time':
                date = block.find('p').text.strip()
                break
        
        event_dates.append(date)  # Append the found date or None if no date was found
        
        # Sleep for a bit to avoid hitting the server too hard
        time.sleep(1)

    # Create a DataFrame
    master_df = pd.DataFrame({
        "event_name": all_events,
        "event_link": all_links,
        "event_dates": event_dates
    })

    return master_df

def process_data(df):
    # Create a temporary DataFrame by splitting the event_dates column into date portion and additional text
    df['event_dates'] = df['event_dates'].str.split('\n', expand=True)[0]

    # Create a temporary DataFrame by splitting the event_dates column into start_date and end_date
    temp_df = df['event_dates'].str.split(' - ', expand=True, n=1)
    temp_df.columns = ['start_date', 'end_date']

    # Split end_date into end_date and year, if end_date is not None
    temp_df[['end_date', 'year']] = temp_df['end_date'].str.split(',', expand=True, n=1)

    # If end_date is None, set end_date and year to be the same as start_date
    temp_df.loc[temp_df['end_date'].isna(), 'end_date'] = temp_df['start_date']
    temp_df.loc[temp_df['year'].isna(), 'year'] = temp_df['start_date'].str.split(',', expand=True, n=1)[1]

    # Convert 'start_date' and 'year' to string type
    temp_df['start_date'] = temp_df['start_date'].astype(str)
    temp_df['end_date'] = temp_df['end_date'].astype(str)
    temp_df['year'] = temp_df['year'].astype(str)

    # Add the year to start_date
    temp_df['start_date'] = temp_df['start_date'] + ", " + temp_df['year']
    temp_df['end_date'] = temp_df['end_date'] + ", " + temp_df['year']

    # Convert the start_date and end_date columns to datetime
    temp_df['start_date'] = pd.to_datetime(temp_df['start_date'], errors='coerce')
    temp_df['end_date'] = pd.to_datetime(temp_df['end_date'], errors='coerce')

    # If end_date is NaT (Not a Time), copy the start_date to end_date
    temp_df['end_date'] = temp_df['end_date'].where(temp_df['end_date'].notna(), temp_df['start_date'])

    # Join the temporary DataFrame with the original DataFrame
    df = df.join(temp_df)

    # Format the start_date and end_date columns in the 'YYYY-MM-DD' format
    df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
    df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')

    df = df.drop(columns=['event_link','year','event_dates'])

    df["record_load_timestamp"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    return df

def upload_to_s3(df, bucket, path):
    # Format the current date and time
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Create the full S3 path with date partitioning
    s3_path = f"{path}date={current_date}/major_events_{current_datetime}.csv"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)

    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, s3_path).put(Body=csv_buffer.getvalue())


def lambda_handler(event, context):
    try:
        df = fetch_data()
        df = process_data(df)
        upload_to_s3(df, BUCKET_NAME, BUCKET_PATH)
    except Exception as e:
        print(f"An error occurred: {e}")
        raise