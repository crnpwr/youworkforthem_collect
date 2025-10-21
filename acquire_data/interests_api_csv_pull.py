import pandas as pd
import requests
import zipfile
from scrape_logging import setup_logger
from scrape_logging import update_last_updates

logger = setup_logger()

def download_and_extract_zip(zip_url, extract_to):
    # Download a ZIP file from the given URL and extract its contents to the specified directory.
    # Delete the ZIP file after extraction.

    try:
        # Download the ZIP file
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses

        # Save the ZIP file locally
        zip_path = os.path.join(extract_to, "temp.zip")
        os.makedirs(extract_to, exist_ok=True)
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract the contents of the ZIP file
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

        logger.info(f"ZIP file downloaded and extracted to: {extract_to}")

        # Remove the temporary ZIP file
        os.remove(zip_path)
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while downloading ZIP file: {e}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting ZIP file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    return False


def check_all_registers():
    url = "https://interests-api.parliament.uk/api/v1/Registers?Skip=0&Take=2000"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        if not data or "items" not in data or not data["items"]:
            print("No items found in the response.")
            logger.error("No items found in the response.")
            return None

        available_versions_info = data["items"]
        return available_versions_info

    except Exception as e:
        logger.error(f"Error checking for updates: {e}")
        return None


def combine_csvs(most_recent_id, archive_folder="data_archive/interests/", output_folder="data_raw/interests/"):
    # Combine multiple CSV files into a single CSV file.

    archive_subfolders = [f.name for f in os.scandir(archive_folder) if f.is_dir() and f.name.startswith(f"register_")]
    # Sort archive_subfolders by register ID in descending order
    archive_subfolders.sort(key=lambda x: int(x.split("_")[1]), reverse=True)

    # Take the newest folder as the 'base' to combine others into
    source_folder = None
    for folder in archive_subfolders:
        if folder.startswith(f"register_{most_recent_id}_"):
            source_folder = os.path.join(archive_folder, folder)
            # Remove source_folder from archive_subfolders to avoid re-processing
            archive_subfolders.remove(folder)
            break

    if not source_folder:
        logger.error(f"No folder found for register ID {most_recent_id}, cannot combine CSVs and complete update.")
        return

    csv_cat_list = ['1', '1.1', '1.2', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    for cat in csv_cat_list:
        newest_record = f"{source_folder}/PublishedInterest-Category_{cat}.csv"
        output_file = f"{output_folder}/PublishedInterest-Category_{cat}.csv"

        df = pd.read_csv(newest_record)
        for folder in archive_subfolders:
            older_csv_file = f"{archive_folder}{folder}/PublishedInterest-Category_{cat}.csv"
            older_df = pd.read_csv(older_csv_file)
            # Add older listings
            df = concat_older(df, older_df, cat)
            # If there is any difference in headers between df and older_df, log a warning
            if set(df.columns) != set(older_df.columns):
                logger.warning(f"Different headers found between {newest_record} and {older_csv_file}")
                logger.warning(f"Headers in {newest_record}: {df.columns.tolist()}")
                logger.warning(f"Headers in {older_csv_file}: {older_df.columns.tolist()}")

        df.to_csv(output_file, index=False)

def concat_older(df, older_df, csv_cat):
    """ Concatenate older_df to df, ensuring no duplicates based on 'ID'.
    Interests are struck from the register after a year, but we want to see the full history.
    For some categories, we require an end date from the older CSV to be present.
    This is because in some cases, an older record may have been deleted because it was erroneous rather than expired.
    e.g. Nigel Farage's work for GB News - which was originally misreported, then removed from later registers.
    """
    required_end_col = False
    if csv_cat in ['1.2']:
        required_end_col = "EndDate"

    if not required_end_col:
        combined_df = pd.concat([df, older_df[~older_df['ID'].isin(df['ID'])]], ignore_index=True)
    else:
        combined_df = pd.concat([df, older_df[(~older_df['ID'].isin(df['ID'])) & (older_df[required_end_col].notna())]], ignore_index=True)
    return combined_df

def update_interests(extract_to="data_raw/interests/",
                     last_updates_file="acquire_data/last_updates.json",
                     archive_folder="data_archive/interests/"):

    logger.info("Starting to update interests data...")
    available_registers_info = check_all_registers()

    # Check for existing downloaded versions in the archive folder
    archive_subfolders = [f.name for f in os.scandir(archive_folder) if f.is_dir()]
    downloaded_versions = []
    for folder in archive_subfolders:
        if folder.startswith("register_"):
            try:
                downloaded_versions.append(int(folder.split("_")[1]))
            except ValueError:
                print(f"Can't get version number from folder name: {folder}")

    # Check for non-downloaded versions
    non_downloaded_versions = [reg["id"] for reg in available_registers_info if reg["id"] not in downloaded_versions]
    if len(non_downloaded_versions) > 0:
        print(f"{len(non_downloaded_versions)} new register versions to download: {non_downloaded_versions}")
        logger.info(f"{len(non_downloaded_versions)} new register versions to download: {non_downloaded_versions}")
    else:
        logger.info("No new updates available.")

    for reg in available_registers_info:
        if reg["id"] in non_downloaded_versions:
            logger.info(f"Downloading register {reg['id']} published on {reg['publishedDate']}.")

            reg_download = ""
            for link in reg["links"]:
                if link["rel"] == "csv":
                    reg_download = link["href"]
                    break

            # Download and extract CSV files
            if reg_download:
                download_and_extract_zip(reg_download,
                                         f"{archive_folder}register_{reg["id"]}_"
                                         f"{reg["publishedDate"]}/")
            else:
                logger.error(f"No CSV download link found for register {reg['id']}.")

    # Combine CSVs to ensure the latest data is in the raw data folder
    combine_csvs(most_recent_id=available_registers_info[0]["id"])

    # Update the last updates JSON file
    most_recent_available_info = {
        "id": available_registers_info[0]["id"],
        "publication_date": available_registers_info[0]["publishedDate"],
        "datetime": f"{available_registers_info[0]["publishedDate"]}T00_00_00"
    }
    update_last_updates(last_updates_file, "interests", most_recent_available_info)



if __name__ == "__main__":
    # Run from mp_collect directory
    import os

    # Set the working directory to the parent directory of the parent directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Update interests data
    update_interests()