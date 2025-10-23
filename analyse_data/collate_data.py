# Here we'll collate the basic data from the IPSA, Parliament register of interests and voting records.
# This will form the backbone of the data for the MPs, giving data structure.
# Basic data includes their party, constituency, and thumbnail image.

import json
import logging
import pandas as pd

def ipsa_basic(ipsa_json):
    """
    Collate basic MP information from IPSA data.

    Args:
        ipsa_json (str): Path to the IPSA JSON file containing MP data.

    Returns:
        pd.DataFrame: A DataFrame containing basic information about MPs.
    """
    logging.info("Collating basic MP information from IPSA data...")

    with open(ipsa_json, "r") as f:
        mps_data_ipsa = json.load(f)

    # Retrieve basic MP information
    mps_info = {}
    for mp_id, mp_data in mps_data_ipsa.items():
        mps_info[mp_id] = {
            "name": mp_data.get("name", ""),
            # Convert party name from Labour (Co-op) to Labour for consistency
            "party": mp_data["latestParty"]["name"].replace(" (Co-op)", ""),
            "party_id": mp_data["latestParty"]["id"],
            "constituency": mp_data["latestHouseMembership"]["membershipFrom"],
            "thumbnail": mp_data.get("thumbnailUrl", ""),
            "gender": mp_data.get("gender", ""),
            "current_membership_since": mp_data["latestHouseMembership"]["membershipStartDate"],
            "salary_since_apr25": 93904,
            "salary_since_apr24": 91346,
        }

        mps_expenses = mp_data.get("expenses", [])
        # Calculate total expenses claimed since the last election
        total_expenses = sum(expense["amountClaimed"] for expense in mps_expenses if expense["date"] >= "2024-07-04T00:00:00")
        mps_info[mp_id]["expenses_total"] = total_expenses

        # Add expense totals by category and subcategory
        for expense in mps_expenses:
            category = expense["category"]
            if f"expenses_{category}" not in mps_info[mp_id]:
                mps_info[mp_id][f"expenses_{category}"] = 0
            mps_info[mp_id][f"expenses_{category}"] += expense["amountClaimed"]

            # Add selected subcategories
            if category == "Accommodation":
                accommodation_subcategories = ["Cleaning services", "Utilities"]

                subcategory = expense.get("expenseType", "")
                if subcategory in accommodation_subcategories:
                    if f"expenses_{category}_{subcategory}" not in mps_info[mp_id]:
                        mps_info[mp_id][f"expenses_{category}_{subcategory}"] = 0
                    mps_info[mp_id][f"expenses_{category}_{subcategory}"] += expense["amountClaimed"]

            ### Commented out section allows for all expense subcategories, but too much data for final output.
            #subcategory = expense.get("expenseType", "")
            #if subcategory:
            #    if f"expenses_{category}_{subcategory}" not in mps_info[mp_id]:
            #        mps_info[mp_id][f"expenses_{category}_{subcategory}"] = 0
            #    mps_info[mp_id][f"expenses_{category}_{subcategory}"] += expense["amountClaimed"]


    # Create a DataFrame for easier manipulation and export
    mps_df = pd.DataFrame.from_dict(mps_info, orient='index')

    # Add a true/false filter for whether the MP has claimed expenses for their own utility bills
    mps_df["claimed_for_utilities"] = mps_df["expenses_Accommodation_Utilities"] > 0

    # Fill NaN values with 0 for expenses
    mps_df.fillna(0, inplace=True)
    return mps_df


def df_rank_and_percentile(df, column, ascending=False):
    """
    Rank the DataFrame based on a specified column and calculate percentiles.

    Args:
        df (pd.DataFrame): The DataFrame to rank.
        column (str): The column to rank by.
        ascending (bool): Whether to rank in ascending order.

    Returns:
        pd.DataFrame: The DataFrame with additional 'rank' and 'percentile' columns.
    """

    df = df.copy()
    df[f'{column}_rank'] = df[column].rank(method='min', ascending=ascending)
    df[f'{column}_percentile'] = df[f'{column}_rank'].rank(method='max', pct=True, ascending=ascending)

    return df


def multiple_columns_rank_and_percentile(df, columns, prefix, ascending=False):
    """
    Rank multiple columns in the DataFrame and calculate percentiles.

    Args:
        df (pd.DataFrame): The DataFrame to rank.
        columns (list): List of columns to rank.
        prefix (str): Prefix to find columns in the DataFrame.
        ascending (bool): Whether to rank in ascending order.

    Returns:
        pd.DataFrame: The DataFrame with additional 'rank' and 'percentile' columns for each specified column.
    """
    if columns:
        for column in columns:
            df = df_rank_and_percentile(df, column, ascending=ascending)
            df.rename(columns={f'{column}_rank': f'{prefix}_{column}_rank',
                               f'{column}_percentile': f'{prefix}_{column}_percentile'}, inplace=True)
    elif prefix:
        # If no specific columns are provided, rank all columns starting with the prefix
        for column in df.columns:
            if column.startswith(prefix):
                df = df_rank_and_percentile(df, column, ascending=ascending)

    return df


def collate_vote_data(df, vote_id, interesting_values=[]):
    """
    Collate vote data for MPs based on a specific vote ID.

    Args:
        df (pd.DataFrame): The DataFrame containing MP data.
        vote_id (str): The ID of the vote to collate.
        interesting_values (list): List of values to filter the votes by. (i.e. 'Aye', 'No', 'Absent')

    Returns:
        pd.DataFrame: A DataFrame with the collated vote data.
    """
    vote_folder = f"data_raw/votes/{vote_id}/"
    #vote_files = [f"{vote_id} - {response}.txt" for response in ["ayes", "noes", "novoterecorded"]]

    # Add response to {vote_id}_response column
    response_column = f"vote_{vote_id}_response"
    if response_column not in df.columns:
        df[response_column] = ""

    vote_responses = ["ayes", "noes", "novoterecorded"]

    for response in vote_responses:
        with open(vote_folder + f"{vote_id} - {response}.txt", 'r') as f:
            members = f.read().splitlines()
            members = [member.strip() for member in members if member.strip()]

        for member in members:
            if member not in df.index:
                logging.warning(f"Member {member} not found in DataFrame for vote {vote_id}.")
                continue

            # Set the response for the member
            df.at[member, response_column] = response
            # If response is in interesting_values, set the filter column
            filter_column = f"vote_{vote_id}_response_filter"
            if response in interesting_values:
                if filter_column not in df.columns:
                    df[filter_column] = False
                df.at[member, filter_column] = True


    return df


def collate_landlord_info(df, property_csv):
    """
    Collate landlord information from a CSV file into the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing MP data.
        property_csv (str): Path to the CSV file containing landlord information.

    Returns:
        pd.DataFrame: The DataFrame with additional landlord information.
    """
    logging.info("Collating landlord information...")

    # Read the landlord CSV file
    landlords_df = pd.read_csv(property_csv, index_col="ID")

    # Filter out all rows where "RentalIncomeEndDate" is not null and is before today
    landlords_df = landlords_df[~((landlords_df["RentalIncomeEndDate"].notnull()) & (
                pd.to_datetime(landlords_df["RentalIncomeEndDate"]) < pd.Timestamp.now()))]
    landlords_df = landlords_df[~((landlords_df["EndDate"].notnull()) & (
                pd.to_datetime(landlords_df["EndDate"]) < pd.Timestamp.now()))]

    # For rows where 'NumberOfProperties' is empty, set it to 1
    landlords_df["NumberOfProperties"] = landlords_df["NumberOfProperties"].fillna(1).astype(int)

    # Sum all "NumberofProperties" by MNIS_ID
    property_owners_df = landlords_df.groupby("MNIS ID")["NumberOfProperties"].sum().reset_index()
    # Create a field for number of properties where RegistrableRentalIncome is "True"
    rental_properties_df = landlords_df[landlords_df["RegistrableRentalIncome"] == True].groupby("MNIS ID")["NumberOfProperties"].sum().reset_index()
    # Rename columns for clarity
    property_owners_df.rename(columns={"NumberOfProperties": "TotalProperties"}, inplace=True)
    rental_properties_df.rename(columns={"NumberOfProperties": "RentalProperties"}, inplace=True)
    # Convert the MNIS ID to string for consistency
    property_owners_df["MNIS ID"] = property_owners_df["MNIS ID"].astype(str)
    rental_properties_df["MNIS ID"] = rental_properties_df["MNIS ID"].astype(str)

    # Add a new column to the main DataFrame for total properties
    df["TotalProperties"] = 0
    df["RentalProperties"] = 0
    # Update the DataFrame with the total properties and rental properties
    for index, row in property_owners_df.iterrows():
        if row["MNIS ID"] in df.index:
            df.at[row["MNIS ID"], "TotalProperties"] = row["TotalProperties"]
    for index, row in rental_properties_df.iterrows():
        if row["MNIS ID"] in df.index:
            df.at[row["MNIS ID"], "RentalProperties"] = row["RentalProperties"]

    # Add a column for whether the MP is a landlord
    df["is_landlord"] = df["RentalProperties"] > 0

    return df






def update_last_updated(last_updates_file="acquire_data/last_updates.json", data_reference_file="data/data_ref.json"):
    """
    :param last_updates_file: Location of last updates record for data acquisition
    :param data_reference_file: Location of file storing record of data collection to be used by dataviz.
    :return: None
    """
    with open(last_updates_file, "r") as f:
        src_data = json.load(f)

    update_times = [src_data[x]["datetime"] for x in src_data if "datetime" in src_data[x]]
    most_recent_update = max(update_times)
    last_update_str = f"{most_recent_update[8:10]}/{most_recent_update[5:7]}/{most_recent_update[0:4]}"

    with open(data_reference_file, "r") as f:
        ref_data = json.load(f)
        ref_data["last_updated"] = last_update_str

    with open(data_reference_file, "w") as f:
        json.dump(ref_data, f, indent=2)


def collate_data(ipsa_json_file="data_raw/expenses/mp_data_ipsa_filtered.json",
                 output_csv_file="data/mp_data_summary.csv"):

    # Call the function to collate basic MP information
    mps_basic_info = ipsa_basic(ipsa_json_file)

    mps_basic_info = multiple_columns_rank_and_percentile(mps_basic_info, [], "expenses_", ascending=False)

    # Collate vote data for specific votes
    votes = [
        ("1841", ["noes"]),  # Winter Fuel Payment
        ("1905", ["noes"]),  # Renters Rights Bill
        ("2074", ["ayes"]),  # UC and PIP Bill Second Reading
        ("2078", ["ayes"]),  # Terrorism Act Amendment, Proscribing Palestine Action
        ("2083", ["noes"]),  # Football Governance Bill
    ]
    for vote_id, interesting_values in votes:
        mps_basic_info = collate_vote_data(mps_basic_info, vote_id, interesting_values)

    mps_basic_info = collate_landlord_info(mps_basic_info,
                                           property_csv="data_raw/interests/PublishedInterest-Category_6.csv")

    # Save the DataFrame to a CSV file
    mps_basic_info.to_csv(output_csv_file, index_label="mp_id")
    update_last_updated()

if __name__ == "__main__":
    # Run from mp_collect directory
    import os

    # Set the working directory to the parent directory of the parent directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    logging.basicConfig(level=logging.INFO)

    ipsa_json = "data_raw/expenses/mp_data_ipsa_filtered.json"
    output_csv = "data/mp_data_summary.csv"

    collate_data(ipsa_json_file = ipsa_json, output_csv_file = output_csv)

    logging.info(f"Basic MP information collated and saved to {output_csv}.")