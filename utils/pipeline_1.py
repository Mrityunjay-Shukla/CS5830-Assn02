import os
import sys
from datetime import datetime
import logging
import random
import shutil

import requests
from bs4 import BeautifulSoup

# setup the working directory
working_dir = os.path.expanduser("~/CS5830/Assignment2/")
if not os.path.exists(working_dir + "tmp"):
    os.makedirs(working_dir + "tmp")

def select_random_csv_urls(**kwargs):
    """
    Selects random CSV file URLs from fetched HTML content based on user input.

    Parameters:
    **kwargs (dict): Keyword arguments containing the DAG run configuration.

    Returns:
    list: List of randomly selected CSV file URLs.
    """
    # Fetch user input parameters
    selected_year = str(kwargs["dag_run"].conf["year"])
    num_samples = kwargs["dag_run"].conf["num_samples"]

    # Validate the selected year
    if not (1901 <= int(selected_year) <= 2024):
        logging.error("Invalid year. Year must be between 1901 and 2024.")
        return []

    # Validate the number of samples
    if num_samples < 1:
        logging.error("Invalid num_samples. Number of samples must be greater than 0.")
        return []

    logging.info("Received user input for year and num_samples.")

    # Define base URL for NOAA datasets
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"

    # Path to the HTML file containing dataset links
    html_file_path = working_dir + "tmp/datasets_new.html

    # Read HTML content from file
    try:
        with open(html_file_path, "r") as f:
            html_content = f.read()
    except FileNotFoundError:
        logging.error(f"HTML file not found at path: {html_file_path}")
        return []

    # Parse HTML content to extract CSV file URLs
    soup = BeautifulSoup(html_content, "html.parser")
    csv_links = [link["href"] for link in soup.find_all("a") if link["href"].endswith(".csv")]

    logging.info("Parsed HTML content to extract CSV file URLs.")

    # Sample CSV file URLs
    sampled_csv_links = random.sample(csv_links, min(num_samples, len(csv_links)))

    # Construct full URLs for sampled CSV files
    selected_csv_urls = [base_url + selected_year + "/" + csv_link for csv_link in sampled_csv_links]
    logging.info(f"Generated selected CSV URLs: {selected_csv_urls}")

    return selected_csv_urls




def create_csv_archive():
    """
    Creates a zip archive of CSV files in the 'tmp/csv_files' directory
    and saves it to the 'tmp/archive_files' directory.
    """

    # Define directory paths
    zip_dir_name = "csv_archive"
    zip_dir_path = os.path.join(os.getenv("WORKING_DIR", ""), zip_dir_name)

    # Create the zip archive directory if it doesn't exist
    if not os.path.exists(zip_dir_path):
        print("Creating zip archive directory.")
        os.makedirs(zip_dir_path)

    # Define source and destination paths for zipping
    source_path = os.path.join(os.getenv("WORKING_DIR", ""), "tmp/csv_files/")
    destination_path = os.path.join(os.getenv("WORKING_DIR", ""), "tmp/archive_files")

    # Create a zip archive of CSV files
    shutil.make_archive(destination_path, "zip", source_path)

    # Remove the temporary zip archive directory
    shutil.rmtree(zip_dir_path)

    return



def download_sampled_csv_files(task_instance):
    """
    Downloads individual CSV files based on the sampled URLs retrieved from the previous task.

    Parameters:
    task_instance (TaskInstance): Airflow TaskInstance object containing task details.

    Returns:
    None
    """
    # Retrieve sampled CSV file URLs from XCom
    sampled_urls = task_instance.xcom_pull(task_ids="select_random_csv_urls")
    sampled_urls = sampled_urls[0]  # Unpack the list from XCom result
    logging.info(f"Retrieved sampled URLs: {sampled_urls}")

    # Define the directory to save downloaded CSV files
    save_directory = os.path.join(os.getenv("WORKING_DIR", ""), "tmp/csv_files/")

    # Ensure the directory exists or create it if not
    if not os.path.exists(save_directory):
        logging.info(f"Creating directory: {save_directory}")
        os.makedirs(save_directory)
    else:
        logging.info(f"Directory already exists at: {save_directory}. Clearing...")
        shutil.rmtree(save_directory)
        os.makedirs(save_directory)

    # Download and save each sampled CSV file
    for url in sampled_urls:
        filename = url.split("/")[-1]
        file_path = os.path.join(save_directory, filename)

        try:
            response = requests.get(url)
            with open(file_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Downloaded: {filename}")
        except Exception as e:
            logging.error(f"Failed to download {filename}: {e}")

    logging.info("Downloaded all sampled CSV files.")
    return



def move_archive_to_destination(**kwargs):
    """
    Moves the archive file to the specified destination directory.

    Parameters:
    **kwargs (dict): Keyword arguments containing the DAG run configuration.

    Returns:
    None
    """
    # Retrieve the destination directory from DAG run configuration
    destination_dir = kwargs["dag_run"].conf["destination_dir"]
    destination_path = os.path.join(os.getenv("WORKING_DIR", ""), destination_dir)

    # Ensure the destination directory exists
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    # Move the archive file to the destination directory
    source_archive_path = os.path.join(os.getenv("WORKING_DIR", ""), "tmp/archive_files.zip")
    destination_archive_path = os.path.join(destination_path, "data_files.zip")

    try:
        shutil.move(source_archive_path, destination_archive_path)
        print(f"Archive moved to: {destination_archive_path}")
    except Exception as e:
        print(f"Error moving archive: {e}")

    return

