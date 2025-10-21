# This script scrapes voting records from the UK Parliament website.
# It's a single-run script that collects for one vote at a time, as specified by the user.
# Hard to see a use case for this being done at scale, so we'll keep it simple and interface directly with the user.

import requests
import json
import os

def get_vote_record(vote_id):
    url = f"https://commonsvotes-api.parliament.uk/data/division/{vote_id}.json"
    print(url)

    try:
        # Open URL
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

        # Parse JSON data
        data = response.json()

        title = data.get('Title', 'No title found')
        print(f"Vote ID: {vote_id}, Title: {title}")

        return data

    except Exception as e:
        print(f"Error retrieving vote record for ID {vote_id}: {e}")
        return None


def save_vote_record(data, output_folder=f"data_raw/votes"):
    # Make the folder output_folder if it doesn't exist
    output_folder = output_folder.rstrip('/')  # Ensure no trailing slash
    division_id = data.get('DivisionId', 'unknown_division_id')
    output_folder = f"{output_folder}/{division_id}"
    title = data.get('Title', 'No title found')

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Make sure the title is a valid filename
    title = ''.join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
    # Save the vote record to a JSON file
    json_output = f"{output_folder}/{division_id} - {title}.json"
    with open(json_output, "w") as f:
        json.dump(data, f, indent=4)

    ayes = [x['MemberId'] for x in data.get('Ayes', [])]
    aye_tellers = [x['MemberId'] for x in data.get('AyeTellers', [])]
    noes = [x['MemberId'] for x in data.get('Noes', [])]
    no_tellers = [x['MemberId'] for x in data.get('NoTellers', [])]
    novoterecorded = [x['MemberId'] for x in data.get('NoVoteRecorded', [])]

    # For simplicity, we're including aye_tellers in the ayes list and no_tellers in the noes list
    # Based on the notion of tellers only "technically" not-voting below, and the associated frustration expressed.
    # https://www.vickyfoxcroft.org.uk/what-is-telling/
    ayes.extend(aye_tellers)
    noes.extend(no_tellers)

    # Save the lists of members who voted each way
    with open(f"{output_folder}/{division_id} - ayes.txt", "w") as f:
        for member_id in ayes:
            f.write(f"{member_id}\n")
    with open(f"{output_folder}/{division_id} - noes.txt", "w") as f:
        for member_id in noes:
            f.write(f"{member_id}\n")
    with open(f"{output_folder}/{division_id} - novoterecorded.txt", "w") as f:
        for member_id in novoterecorded:
            f.write(f"{member_id}\n")

    print(f"Vote record for ID {division_id} saved successfully.")


if __name__ == "__main__":
    # Run from mp_collect directory
    import os
    # Set the working directory to the parent directory of the parent directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    vote_id = input("Enter the vote ID: ")
    vote_record = get_vote_record(vote_id)
    save_vote_record(vote_record)

    print("Voting record saved, don't forget to add to analyse_data and collate_data as necessary.")

