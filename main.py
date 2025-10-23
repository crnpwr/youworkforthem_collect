# The aim of main will ultimately be to update data and process it as necessary.

from acquire_data.interests_api_csv_pull import update_interests
from acquire_data.expenses_web_scrape import get_mps_ipsa_data, filter_and_copy_ipsa_data
import logging
from analyse_data.collate_data import collate_data
from analyse_data.analyse_data import add_personal_analysis

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Updating interests...")
    update_interests()
    print("Updating expenses...")
    get_mps_ipsa_data()
    filter_and_copy_ipsa_data()

    print("Collating data...")
    ipsa_json = "data_raw/expenses/mp_data_ipsa_filtered.json"
    output_csv = "data/mp_data_summary.csv"
    collate_data(ipsa_json_file=ipsa_json, output_csv_file=output_csv)

    print("Analysing data...")
    add_personal_analysis()

    print("All tasks completed.")

