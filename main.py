# The aim of main will ultimately be to update data and process it as necessary.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from acquire_data.interests_api_csv_pull import update_interests
from acquire_data.expenses_web_scrape import get_mps_ipsa_data, filter_and_copy_ipsa_data
import logging
from analyse_data.collate_data import ipsa_basic, multiple_columns_rank_and_percentile, collate_vote_data, collate_landlord_info
from analyse_data.analyse_data import add_personal_analysis


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    update_interests()
    get_mps_ipsa_data()
    filter_and_copy_ipsa_data()

    # Collate currently messy, need to tidy it up.
    ipsa_json_file = "data_raw/expenses/mp_data_ipsa_filtered.json"

    # Call the function to collate basic MP information
    mps_basic_info = ipsa_basic(ipsa_json_file)

    mps_basic_info = multiple_columns_rank_and_percentile(mps_basic_info, [], "expenses_", ascending=False)

    # Collate vote data for specific votes
    votes = [("1841", ["noes"]), ("1905", ["noes"])]
    for vote_id, interesting_values in votes:
        mps_basic_info = collate_vote_data(mps_basic_info, vote_id, interesting_values)

    mps_basic_info = collate_landlord_info(mps_basic_info,
                                           property_csv="data_raw/interests/PublishedInterest-Category_6.csv")

    # Save the DataFrame to a CSV file
    output_csv_file = "data/mp_data_summary.csv"
    mps_basic_info.to_csv(output_csv_file, index_label="mp_id")

    # Add personal analysis
    add_personal_analysis()

