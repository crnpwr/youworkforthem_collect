import requests
from bs4 import BeautifulSoup
import json
from .scrape_logging import setup_logger, update_last_updates

logger = setup_logger()

def get_mp_list(mp_ids_file="acquire_data/mp_ids.txt"):
    # Get a list of all MPs IDs from "mp_ids.txt" as strings

    with open(mp_ids_file, "r") as f:
        mp_ids = f.read().strip().split("\n")

    return mp_ids


def get_mp_data_ipsa(mp_id):
    # Get MP expense data from IPSA for an individual MP from their ID.
    url = f"https://www.theipsa.org.uk/mp-staffing-business-costs/your-mp/x/{mp_id}"

    try:
        # Open URL
        r = requests.get(url, timeout=10)
        r.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

        soup = BeautifulSoup(r.text, 'html.parser')
        next_data = soup.find(id="__NEXT_DATA__")

        if not next_data:
            logger.error(f"MP {mp_id}: Missing '__NEXT_DATA__' element.")
            return None

        # Extract and parse JSON data
        json_data = next_data.string
        data = json.loads(json_data)
        mp_info = data["props"]["pageProps"]["mp"]

        return mp_info

    except requests.exceptions.RequestException as e:
        logger.error(f"MP {mp_id}: Request error - {e}")
    except json.JSONDecodeError as e:
        logger.error(f"MP {mp_id}: JSON parsing error - {e}")
    except Exception as e:
        logger.error(f"MP {mp_id}: Unexpected error - {e}")

    return None


def get_mps_ipsa_data(
        ipsa_json_file="data_raw/expenses/mp_data_ipsa.json",
        archive_folder="data_archive/expenses/",
        last_updates_file="acquire_data/last_updates.json"
):
    # Get MP data from IPSA.
    # Includes expenses, as well as information on party

    logger.info("Starting to retrieve MP data from IPSA...")
    full_mp_list = get_mp_list()
    print(f"Total MPs: {len(full_mp_list)}")

    mps_data_ipsa = {}
    for mp_id in full_mp_list:
        mp_data = get_mp_data_ipsa(mp_id)
        if mp_data:
            mps_data_ipsa[mp_id] = mp_data
        else:
            logger.warning(f"Failed to retrieve data for MP ID: {mp_id}")
            print(f"Failed to retrieve data for MP ID: {mp_id}")

    # Create json file
    with open(ipsa_json_file, "w") as f:
        json.dump(mps_data_ipsa, f, indent=2)

    logger.info("IPSA data retrieval complete. Data saved to 'mp_data_ipsa.json'.")

    # Create a copy in the archive folder
    # Make an archive subfolder like "2025-05-22T18_10_06-ScrapedExpense"
    import os
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
    archive_subfolder = os.path.join(archive_folder, f"{timestamp}-ScrapedExpense")
    os.makedirs(archive_subfolder, exist_ok=True)
    archive_file_path = os.path.join(archive_subfolder, "mp_data_ipsa.json")
    with open(archive_file_path, "w") as f:
        json.dump(mps_data_ipsa, f, indent=2)

    # Update record of last updates
    update_last_updates(last_updates_file, "expenses", {"datetime" : timestamp})



def filter_and_copy_ipsa_data(
        ipsa_json_file="data_raw/expenses/mp_data_ipsa.json",
        filtered_ipsa_json_file="data_raw/expenses/mp_data_ipsa_filtered.json"):

    # Load the MP data from the JSON file
    with open(ipsa_json_file, "r") as f:
        mps_data_ipsa = json.load(f)

    # Filter expenses since last election, for comparability
    expenses_since = "2024-07-04T00:00:00"

    output_json = {}
    for mp_id in mps_data_ipsa:
        mp_data = mps_data_ipsa[mp_id]
        mp_data_out = {}
        for key in mp_data:
            if key not in ["expenses", "history"]:
                mp_data_out[key] = mp_data[key]
            elif key == "expenses":
                # Filter expenses since a certain date
                filtered_expenses = [expense for expense in mp_data[key] if expense["date"] >= expenses_since]
                if filtered_expenses:
                    mp_data_out[key] = filtered_expenses

        output_json[mp_id] = mp_data_out

    # Save the filtered data to a new JSON file
    with open(filtered_ipsa_json_file, "w") as f:
        json.dump(output_json, f, indent=2)


if __name__ == "__main__":
    # Run from mp_collect directory
    import os
    # Set the working directory to the parent directory of the parent directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    get_mps_ipsa_data()
    filter_and_copy_ipsa_data()
