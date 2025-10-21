# This takes collated data and adds personal analysis and summary for each MP.

import pandas as pd

def add_personal_analysis(data_file="data/mp_data_summary.csv"):
    """
    Add personal analysis and summary for each MP based on the data file.

    Args:
        data_file (str): Path to the CSV file containing MP data.

    Returns:
        None: The function modifies the CSV file in place.
    """
    # Dataframe from CSV file
    df = pd.read_csv(data_file, encoding='utf-8')

    df['Basic Info'] = df.apply(mp_basic_info, axis=1)
    df[['Property Analysis', 'Property Score']] = df.apply(landlord_and_property_statement, axis=1)
    df = hospitality_analysis(df)
    df = outside_earnings_analysis(df)
    df[['Other Analysis', 'Other Score']] = df.apply(other_statement, axis=1)
    df[['Outside Earnings Analysis', 'Outside Earnings Score']] = df.apply(outside_earnings_statement, axis=1)

    # Create an 'Interesting Score' that sums all the scores.
    score_columns = [col for col in df.columns if col.endswith(' Score')]
    df['Interesting Score'] = df[score_columns].sum(axis=1)

    # Create HTML output for the MP infobox.
    df['mp_infobox_html'] = df.apply(mp_infobox_html, axis=1)

    # Before saving to output, drop columns ending with "_rank" and "_percentile" to avoid clutter.
    columns_to_drop = [col for col in df.columns if col.endswith('_rank') or col.endswith('_percentile')]
    df.drop(columns=columns_to_drop, inplace=True)

    # Convert DataFrame back to CSV
    df.to_csv(data_file, index=False, encoding='utf-8')


def mp_basic_info(r):
    """
    Generate a basic information string for an MP.

    Args:
        r (dict): A dictionary containing MP data.

    Returns:
        str: A basic information string for the MP.
    """
    return f"{r['name']}, {r['party']} MP for {r['constituency']}\n"


def landlord_and_property_statement(r):
    """
    Generate a statement about the MP's landlord and property information.

    Args:
        r (dict): A dictionary containing MP data.

    Returns:
        :return: A pandas Series containing the analysis and score.
    """
    output_lines = []
    output_score = 0

    if r['is_landlord']:
        rental_value = r['RentalProperties'] * 10000 # Rental only has to be listed if > 10k

        if r['vote_1905_response_filter']:
            output_lines.append(f"{r['name']} is a landlord who used his!her power as an MP to vote against the Renters' Rights Bill, which was designed to strengthen the rights of his!her tenants, protecting them from unfair eviction.")
            if r['RentalProperties'] == 1:
                output_lines.append(f"He!She is taking in at least £{rental_value:,} per year from his!her tenants.")
            else:
                output_lines.append(f"He!She has {r['RentalProperties']} listed rental properties, which bring in at least £{rental_value:,} per year.")
            output_score += 5
        else:
            if r['RentalProperties'] == 1:
                output_lines.append(f"{r['name']} is a landlord — he!she is taking in at least £{rental_value:,} per year from his!her tenants.")
            else:
                output_lines.append(f"{r['name']} is a landlord — he!she has {r['RentalProperties']} listed rental properties, which bring in at least £{rental_value:,} per year.")
            output_score += 2

        if r['RentalProperties'] > 2:
            output_score += 1
        elif r['RentalProperties'] == 2:
            output_score += 0.5
    elif r['vote_1905_response_filter']:
        output_lines.append(f"{r['name']} voted against the Renters' Rights Bill, which would have improved tenant rights.")
        output_score += 1

    if r['expenses_Accommodation'] > 10000:
        output_lines.append(f"He!She has claimed £{r['expenses_Accommodation']:0,.0f} in accommodation expenses since the last election.")
        output_score += 0.5
        if r['expenses_Accommodation_rank'] <= 25:
            output_lines.append(f"This means he!she ranks at #{r['expenses_Accommodation_rank']:.0f} for most expensive accommodation charges to the taxpayer.")
            output_score += 1
        elif r['expenses_Accommodation_percentile'] > 0.9:
            output_lines.append(f"This places him!her in the top 10% of MPs for accommodation expenses.")
            output_score += 0.5

    if r['claimed_for_utilities']:
        if r['vote_1841_response_filter']:
            output_lines.append(f"He!She has claimed for his!her utility bills to be paid by the taxpayer. Even so, in 2024 he!she voted to abolish universal winter fuel payments for 10 million pensioners. 2.5 million of these pensioners had 'incomes below levels needed for a dignified life', according to the Centre for Ageing Better.")
            output_score += 2

        if r['expenses_Accommodation_Cleaning services'] > 0:
            output_lines.append(f"He!She has claimed £{r['expenses_Accommodation_Utilities']:0,.0f} for utility bills and £{r['expenses_Accommodation_Cleaning services']:0,.0f} for cleaning services for his!her personal accommodation.")
            output_score += 1
        else:
            output_lines.append(f"He!She has claimed £{r['expenses_Accommodation_Utilities']:0,.0f} for utility bills for his!her personal accommodation.")
            output_score += 0.5

    else:
        if r['expenses_Accommodation_Cleaning services'] > 0:
            output_lines.append(f"He!She has claimed £{r['expenses_Accommodation_Cleaning services']:0,.0f} for cleaning services for his!her personal accommodation.")
            output_score += 0.5

    output_lines = ("\n").join(output_lines)

    return pd.Series([output_lines, output_score], index=["Property Analysis", "Property Score"])


def load_and_filter_hospitality(data_file="data_raw/interests/PublishedInterest-Category_3.csv"):
    """
    Load and filter hospitality data from a CSV file.

    Args:
        data_file (str): Path to the CSV file containing hospitality data.

    Returns:
        pd.DataFrame: A DataFrame containing filtered hospitality data.
    """
    df = pd.read_csv(data_file, encoding='utf-8')
    # Filter out rows where "Registered" is empty or before 2024-07-04
    df = df[df['Registered'].notna() & (df['AcceptedDate'] >= '2024-07-04')] #previously 'registered'

    # Filter out rows where "PaymentDescription" contains "legal advice" or other similar terms.
    # The focus of this analysis is on hospitality, and legal advice, medical advice, feel less relevant.
    # The National Liberal Club appears to be a base for Lib Dem MPs, so we filter it out too, but can see an argument for keeping it.
    filter_out_terms = ['legal advice', 'legal services', 'medical advice', 'medical services', 'medical imagery',
                        'legal costs',' my solicitor', 'the solicitor', 'legal fees', 'appear', 'national liberal club']
    for term in filter_out_terms:
        #df = df[~df['PaymentDescription'].str.contains(term, case=False, na=False)]
        df = df[~df['PaymentDescription'].str.lower().str.contains(term.lower(), case=False, na=False)]

    return df


def hospitality_analysis(df):
    """
    Analyze hospitality data and return a summary.

    Args:
        df (pd.DataFrame): A DataFrame with all MPs data, to which hospitality data will be added.
    Returns:
        pd.DataFrame: A DataFrame with hospitality analysis added.
    """

    filtered_hospitality = load_and_filter_hospitality()
    # Group by MP and sum the hospitality values
    hospitality_summary = filtered_hospitality.groupby('MNIS ID')['Value'].sum().reset_index()

    # Rename the columns for clarity
    hospitality_summary.rename(columns={'Value': 'TotalHospitalityValue'}, inplace=True)

    # For each MP in hospitality_summary, find their entry with the highest value, and copy over the fields 'Value', 'PaymentDescription', and 'DonorName'
    hospitality_summary[['max_hospitality_value', 'max_hospitality_description', 'max_hospitality_donor']] = hospitality_summary.apply(
        lambda row: pd.Series(
            filtered_hospitality[filtered_hospitality['MNIS ID'] == row['MNIS ID']].nlargest(1, 'Value')[['Value', 'PaymentDescription', 'DonorName']].values.flatten()
        ), axis=1
    )

    # Add a new column to hospitality_summary for the number of hospitality entries per MP with a value >= 500, leaving a value of 0 if no hospitality data is available or none >= 500.
    expensive_gifts = filtered_hospitality[filtered_hospitality['Value'] >= 500].groupby('MNIS ID')['Value'].count().reset_index()
    expensive_gifts.rename(columns={'Value': 'expensive_gifts_count'}, inplace=True)
    # Merge the expensive_gifts count into hospitality_summary
    hospitality_summary = hospitality_summary.merge(expensive_gifts, on='MNIS ID', how='left')
    # Fill NaN values with 0 for expensive_gifts_count
    hospitality_summary['expensive_gifts_count'] = hospitality_summary['expensive_gifts_count'].fillna(0)

    # Open a CSV file with donor categories
    donor_file = "analyse_data/donor_categories.csv"
    try:
        donor_categories = pd.read_csv(donor_file, encoding='utf-8')
    except FileNotFoundError:
        print(f"Donor categories file '{donor_file}' not found. Skipping donor categorization.")

    # Categorize donors in filtered_hospitality based on DonorName and DonorCompanyIdentifier.
    # If there's a match for either one, assign the category from donor_categories.
    filtered_hospitality['DonorCategory'] = filtered_hospitality.apply(
        lambda row: donor_categories.loc[
            (donor_categories['DonorName'] == row['DonorName']) |
            (donor_categories['DonorCompanyIdentifier'] == row['DonorCompanyIdentifier']),
            'Category'
        ].values[0] if not donor_categories[(donor_categories['DonorName'] == row['DonorName']) |
                                            (donor_categories['DonorCompanyIdentifier'] == row['DonorCompanyIdentifier'])].empty else '',
        axis=1
    )

    # Add donor categories to MP hospitality summaries.
    filtered_hospitality_categories = filtered_hospitality[filtered_hospitality['DonorCategory'] != '']
    hospitality_categories_by_mp = filtered_hospitality_categories.groupby('MNIS ID')['DonorCategory'].apply(lambda x: ', '.join(set(x))).reset_index()
    hospitality_categories_by_mp.rename(columns={'DonorCategory': 'DonorCategories'}, inplace=True)
    # Merge the donor categories into hospitality_summary
    hospitality_summary = hospitality_summary.merge(hospitality_categories_by_mp, on='MNIS ID', how='left')
    # Add a column to hospitality_summary with a count of unique donor categories for each MP.
    hospitality_summary['DonorCategoriesCount'] = hospitality_summary['DonorCategories'].apply(
        lambda x: len(set(x.split(', '))) if pd.notna(x) else 0
    )

    # Add the data from hospitality summary to the  main DataFrame as values, not a combined string, leaving a value of 0 if no hospitality data is available.
    df = df.merge(hospitality_summary, left_on='mp_id', right_on='MNIS ID', how='left')
    # Delete 'MNIS ID' column as it is not needed in the main DataFrame
    df.drop(columns=['MNIS ID'], inplace=True)
    # Fill NaN values with 0 for hospitality values
    df['TotalHospitalityValue'] = df['TotalHospitalityValue'].fillna(0)
    df['max_hospitality_value'] = df['max_hospitality_value'].fillna(0)
    df['expensive_gifts_count'] = df['expensive_gifts_count'].fillna(0)
    df['DonorCategoriesCount'] = df['DonorCategoriesCount'].fillna(0)
    # If max_hospitality_description is across multiple lines, keep only the first line.
    df['max_hospitality_description'] = df['max_hospitality_description'].apply(lambda x: x.split('\n')[0] if pd.notna(x) else '')
    # If max_hospitality_description contains several sentences, keep only the first sentence.
    df['max_hospitality_description'] = df['max_hospitality_description'].apply(lambda x: x.split('.')[0] if pd.notna(x) else '')

    # Create a new column for rank and percentile of TotalHospitalityValue and expensive_gifts_count.
    df['expenses_HospitalityRank'] = df['TotalHospitalityValue'].rank(ascending=False, method='min')
    df['expenses_HospitalityPercentile'] = df['TotalHospitalityValue'].rank(pct=True, ascending=False)
    df['expenses_ExpensiveGiftsRank'] = df['expensive_gifts_count'].rank(ascending=False, method='min')
    df['expenses_ExpensiveGiftsPercentile'] = df['expensive_gifts_count'].rank(pct=True, ascending=False)

    # Create a 'hospitality score' for each MP.
    df['Hospitality Score'] = ((df['expensive_gifts_count'] * 0.5) + (df['TotalHospitalityValue'] // 1000 * 0.5) +
                               (df['DonorCategoriesCount'] * 0.5))

    df['Hospitality Analysis'] = df.apply(hospitality_statement, axis=1, donor_categories_df=donor_categories)

    return df


def hospitality_statement(r, donor_categories_df):
    """
    Generate a statement about the MP's hospitality information.

    Args:
        r: A row in the DataFrame containing MP data.
        donor_categories_df: A DataFrame containing donor categories.
    :return: A string summarizing the MP's hospitality information.
    """
    output_lines = []

    if r['TotalHospitalityValue'] > 0:
        output_lines.append(f"{r['name']} has received a total of £{r['TotalHospitalityValue']:0,.0f} in hospitality and gifts since the last election.")

        if r['expenses_HospitalityRank'] <= 25:
            output_lines.append(f"This means he!she ranks at #{r['expenses_HospitalityRank']:.0f} for most hospitality received.")
        elif r['expenses_HospitalityPercentile'] > 0.9:
            output_lines.append(f"This places him!her in the top 10% of MPs for hospitality received.")

        if r['max_hospitality_value'] > 0:
            output_lines.append(f"The most expensive declaration was worth £{r['max_hospitality_value']:0,.0f}, described as '{r['max_hospitality_description']}' from {r['max_hospitality_donor']}.")

        if r['expensive_gifts_count'] > 1:
            output_lines.append(f"He!She has received {r['expensive_gifts_count']:.0f} gifts valued at over £500.")

        if r['DonorCategoriesCount'] > 0:
            donor_cat_list = r['DonorCategories'].split(', ')
            if r['DonorCategoriesCount'] > 1:
                donor_cat_list_str = ', '.join(donor_cat_list[:-1]) + f" and {donor_cat_list[-1]}."
            else:
                donor_cat_list_str = donor_cat_list[0] + '.'
            output_lines.append(f"He!She has declared gifts or hospitality from {donor_cat_list_str}")

            for donor_cat in donor_cat_list:
                # Add a line for each donor category, taken from CategorySentence in donor_categories_df
                if donor_cat in donor_categories_df['Category'].values:
                    category_sentence = donor_categories_df.loc[donor_categories_df['Category'] == donor_cat, 'CategorySentence'].values[0]
                    output_lines.append(category_sentence)

    return "\n".join(output_lines)


def other_statement(r):
    """
    Generate a statement about the MP's other information.

    Args:
        r: A row in the DataFrame containing MP data.
    Returns:
        :return: A pandas Series containing the analysis and score.
    """
    output_lines = []
    output_score = 0

    # Broad summary of overall expenses
    if r['expenses_total'] > 0:
        output_lines.append(f"{r['name']} has claimed a total of £{r['expenses_total']:0,.0f} in expenses since the last election.")

        expense_categories = ["expenses_Accommodation", "expenses_Office Costs","expenses_Staffing","expenses_Miscellaneous"]
        nonzero_expenses = [x for x in expense_categories if r[x] > 0]

        if r['expenses_total_rank'] <= 25:
            output_lines.append(
                f"This means he!she ranks at #{r['expenses_total_rank']:.0f} for most expenses claimed.")
        elif r['expenses_total_percentile'] > 0.9:
            output_lines.append(f"This places him!her in the top 10% of MPs for expenses claimed.")

        if len(nonzero_expenses) == 1:
            expense_breakdown = f"His!Her expense total all comes from {nonzero_expenses[0].lower()} costs."
            expense_breakdown = expense_breakdown.replace("costs costs", "costs")
            expense_breakdown = expense_breakdown.replace("expenses_", "")
            output_lines.append(expense_breakdown)
        elif len(nonzero_expenses) > 1:
            expense_breakdown = f"His!Her expense total comes from "
            expense_category_summaries = [f"£{r[x]:,.0f} in {x.lower()} costs" for x in nonzero_expenses]
            expense_breakdown += ', '.join(expense_category_summaries[:-1]) + f" and {expense_category_summaries[-1]}."
            expense_breakdown = expense_breakdown.replace("costs costs", "costs")
            expense_breakdown = expense_breakdown.replace("expenses_", "")
            output_lines.append(expense_breakdown)

    if not r['claimed_for_utilities']:
        if r['vote_1841_response_filter']:
            output_lines.append(f"In 2024, he!she voted to abolish universal winter fuel payments for 10 million pensioners. 2.5 million of these pensioners had 'incomes below levels needed for a dignified life', according to the Centre for Ageing Better.")
            output_score += 0.5

    if r['vote_2074_response_filter']:
        output_lines.append("He!She voted in support of the UC and PIP bill, which will cut the universal credit allowance for many disabled people by £2,080 per year.")
        output_score += 2

    output_lines = "\n".join(output_lines)
    return pd.Series([output_lines, output_score], index=["Other Analysis", "Other Score"])


def outside_earnings_analysis(mp_df,
                          earnings_ref_csv="data_raw/interests/PublishedInterest-Category_1.csv",
                          earnings_adhoc_csv="data_raw/interests/PublishedInterest-Category_1.1.csv",
                          earnings_ongoing_csv="data_raw/interests/PublishedInterest-Category_1.2.csv"):
    """
    Collate earnings information from various CSV files into a DataFrame.
    Create one combined DataFrame with earnings information for MPs.
    One combined 'Value' field will take over, to help understand earnings until current date, taking into account monthly/quarterly/annual earnings.
    :param mp_df: DataFrame containing basic MP information.
    :param earnings_ref_csv:
    :param earnings_adhoc_csv:
    :param earnings_ongoing_csv:
    :return: dataFrame with earnings information for MPs.
    """

    #logging.info("Collating earnings information...")
    # Read the earnings reference CSV file
    earnings_ref_df = pd.read_csv(earnings_ref_csv, index_col="ID")
    # Read the earnings adhoc CSV file
    earnings_adhoc_df = pd.read_csv(earnings_adhoc_csv, index_col="ID")
    # Filter out adhoc earnings where "ReceivedDate" is before 2024-07-04
    earnings_adhoc_df = earnings_adhoc_df[earnings_adhoc_df['ReceivedDate'] >= '2024-07-04']
    # Read the earnings ongoing CSV file
    earnings_ongoing_df = pd.read_csv(earnings_ongoing_csv, index_col="ID")

    # Change 'Value' in earnings_ongoing_df to 'OngoingValue' as this is meant to be a repeatable value, not an all-time value.
    earnings_ongoing_df.rename(columns={"Value": "OngoingValue"}, inplace=True)

    # Check the last time the interests register was updated, using "acquire_data/last_updates.json"
    try:
        with open("acquire_data/last_updates.json", "r") as f:
            last_updates = json.load(f)
            last_update_date = last_updates.get("interests", {}).get("datetime", "")
            last_update_date = last_update_date[:10]  # Get only the date part (YYYY-MM-DD)
    # Use today's date if all else fails
    except:
        #logging.warning("Last updates file not found. Using today's date for earnings calculations.")
        last_update_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    # Add CalculationStartDate to earnings_ongoing_df
    # If StartDate is empty or before 2024-07-04, use 2024-07-04 as the start date.
    earnings_ongoing_df["CalculationStartDate"] = earnings_ongoing_df["StartDate"].fillna("2024-07-04")
    earnings_ongoing_df["CalculationStartDate"] = earnings_ongoing_df["CalculationStartDate"].apply(
        lambda x: x if x >= "2024-07-04" else "2024-07-04"
    )

    # Add CalculationEndDate to earnings_ongoing_df
    # If EndDate is empty or after the last update date, use the last update date as the end date.
    earnings_ongoing_df["CalculationEndDate"] = earnings_ongoing_df["EndDate"].fillna(last_update_date)
    earnings_ongoing_df["CalculationEndDate"] = earnings_ongoing_df["CalculationEndDate"].apply(
        lambda x: x if x <= last_update_date else last_update_date
    )
    # Calculate earnings value for ongoing earnings
    earnings_ongoing_df["Value"] = earnings_ongoing_df.apply(calculate_earnings_value, axis=1)
    # Add a column for Total Hours Worked
    earnings_ongoing_df["TotalHoursWorked"] = earnings_ongoing_df.apply(calculate_hours_worked, axis=1)

    # Combine all earnings_ongoing_df and earnings_adhoc_df into a single DataFrame
    # For any columns that are only in one DataFrame, attach the prefix "Ongoing" or "Adhoc" to the column name and leave empty values.
    earnings_combined_df = pd.concat([earnings_adhoc_df, earnings_ongoing_df], axis=0, ignore_index=False)

    # Sum just the Value field by "Parent Interest ID"
    earnings_combined_df_summed = earnings_combined_df.groupby("Parent Interest ID").agg({"Value": "sum"})

    # Concatenate the Value column into earnings_ref_df
    earnings_ref_df = pd.concat(
        [earnings_ref_df, earnings_combined_df_summed['Value']],
        axis=1
    )
    # Fill empty values in the Value column with 0
    earnings_ref_df["Value"] = earnings_ref_df["Value"].fillna(0)

    earnings_ref_df.to_csv("data/earnings_combined.csv", index_label="ID")

    # Sum earnings by MNIS ID
    earnings_by_mp = earnings_ref_df.groupby("MNIS ID").agg({"Value": "sum"}).reset_index()

    # Count the number of earnings with a value > 0 by MNIS ID
    earnings_count_by_mp = earnings_ref_df[earnings_ref_df["Value"] > 0].groupby("MNIS ID").size().reset_index()

    # Convert the MNIS ID to string for consistency
    earnings_count_by_mp["MNIS ID"] = earnings_count_by_mp["MNIS ID"].astype(int)
    earnings_by_mp["MNIS ID"] = earnings_by_mp["MNIS ID"].astype(int)

    # Add colums for total outside earnings and number of outside sources for each MP.
    mp_df['TotalOutsideEarnings'] = mp_df['mp_id'].map(
        earnings_by_mp.set_index('MNIS ID')['Value']
    ).fillna(0)
    mp_df['TotalOutsideEarningsCount'] = mp_df['mp_id'].map(
        earnings_count_by_mp.set_index('MNIS ID')[0]
    ).fillna(0)


    # Add details of most lucrative earnings source per MP
    mp_df[
        ['TopOutsideEarningsValue', 'TopOutsideEarningsSource', 'TopOutsideEarningsDescription']
    ] = mp_df.apply(
        lambda row: pd.Series(
            earnings_ref_df[earnings_ref_df['MNIS ID'] == row['mp_id']]
            .nlargest(1, 'Value')[['Value', 'PayerName', 'JobTitle']]
            .values.flatten() if not earnings_ref_df[earnings_ref_df['MNIS ID'] == row['mp_id']].empty else (0, "", "")
        ), axis=1
    )

    # Add rank and percentile for TotalOutsideEarnings
    mp_df['TotalOutsideEarnings_rank'] = mp_df['TotalOutsideEarnings'].rank(ascending=False, method='min')
    mp_df['TotalOutsideEarnings_percentile'] = mp_df['TotalOutsideEarnings'].rank(pct=True, ascending=False)

    return mp_df


def calculate_hours_worked(r):
    """
    Calculate the number of hours worked based on:
        PeriodForHoursWorked = "Weekly", "Monthly", "Quarterly", or "Yearly".
        HoursWorked = the amount earned in that period.
        CalculationStartDate = the date from which the earnings are calculated.
        CalculationEndDate = the date until which the earnings are calculated.
    :param r:
    :return: float: Calculated hours worked value.
    """

    if pd.isna(r["PeriodForHoursWorked"]) or pd.isna(r["HoursWorked"]):
        return 0.0  # If no regularity or value, return 0

    # Convert dates to pandas datetime for calculation
    start_date = pd.to_datetime(r["CalculationStartDate"])
    end_date = pd.to_datetime(r["CalculationEndDate"])

    # Depending on the PeriodForHoursWorked, calculate the number of periods between start and end dates
    if start_date > end_date:
        return 0.0
    else:
        if r["PeriodForHoursWorked"] == "Weekly":
            num_periods = (end_date - start_date).days // 7
            return r["HoursWorked"] * num_periods
        elif r["PeriodForHoursWorked"] in ["Monthly", "Quarterly", "Yearly"]:
            # Calculate the number of months between start and end dates, not including fractional months
            num_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
            if r["PeriodForHoursWorked"] == "Monthly":
                return r["HoursWorked"] * num_months
            elif r["PeriodForHoursWorked"] == "Quarterly":
                return r["HoursWorked"] * (num_months / 3)
            elif r["PeriodForHoursWorked"] == "Yearly":
                return r["HoursWorked"] * (num_months / 12)
        else:
            return 0.0



def calculate_earnings_value(r):
    """
    Calculate earnings based on:
        RegularityOfPayment = "Monthly", "Quarterly", or "Yearly".
        OngoingValue = the amount earned in that period.
        CalculationStartDate = the date from which the earnings are calculated.
        CalculationEndDate = the date until which the earnings are calculated.
    :param r:
    :return: float: Calculated earnings value.
    """

    if pd.isna(r["RegularityOfPayment"]) or pd.isna(r["OngoingValue"]):
        return 0.0  # If no regularity or value, return 0

    # Convert dates to pandas datetime for calculation
    start_date = pd.to_datetime(r["CalculationStartDate"])
    end_date = pd.to_datetime(r["CalculationEndDate"])

    # Calculate the number of months between start and end dates, not including fractional months
    if start_date > end_date:
        return 0.0  # If start date is after end date, return 0
    else:
        num_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

    if r["RegularityOfPayment"] == "Monthly":
        return r["OngoingValue"] * num_months
    elif r["RegularityOfPayment"] == "Quarterly":
        return r["OngoingValue"] * (num_months / 3)
    elif r["RegularityOfPayment"] == "Yearly":
        return r["OngoingValue"] * (num_months / 12)
    else:
        return 0.0  # If regularity is not recognized, return 0


def outside_earnings_statement(r):
    """
    Generate a statement about the MP's outside earnings.

    Args:
        r: A row in the DataFrame containing MP data.
    Returns:
        :return: A pandas Series containing the analysis and score.
    """
    output_lines = []
    output_score = 0

    if r['TotalOutsideEarnings'] > 0:
        output_lines.append(f"In addition to his!her parliamentary income, {r['name']} has earned £{r['TotalOutsideEarnings']:0,.0f} since the last election.")

        if r['TotalOutsideEarnings_rank'] <= 25:
            output_lines.append(f"This means he!she ranks at #{r['TotalOutsideEarnings_rank']:.0f} for most outside earnings.")
            output_score += 2.5
        elif r['TotalOutsideEarnings_percentile'] > 0.9:
            output_lines.append(f"This places him!her in the top 10% of MPs for outside earnings.")
            output_score += 1

        #if r['TotalHoursWorked'] > 0:
        #    output_lines.append(f"He!She earned this via {r['TotalHoursWorked']:.0f} hours of work, which is an average of £{r['TotalOutsideEarnings'] / r['TotalHoursWorked']:.0f} per hour.")

        if r["TotalOutsideEarningsCount"] == 1:
            output_lines.append(f"He!She earned all of this money from {r['TopOutsideEarningsSource']}, and his!her role was described as {r["TopOutsideEarningsDescription"]}.")
        else:
            output_lines.append(f"His!Her largest income source outside Parliament was {r['TopOutsideEarningsSource']}, where he!she earned £{r["TopOutsideEarningsValue"]:0,.0f}, with his!her role described as '{r["TopOutsideEarningsDescription"]}'.")

    output_lines = "\n".join(output_lines)
    return pd.Series([output_lines, output_score], index=["Outside Earnings Analysis", "Outside Earnings Score"])


def foreign_trips_analysis(mp_df, trips_csv="data_raw/interests/PublishedInterest-Category_4.csv", filter_since="2024-07-04"):
    """
    Analyze foreign trips data and return a summary.

    Args:
        mp_df (pd.DataFrame): DataFrame containing basic MP information.
        trips_csv (str): Path to the CSV file containing foreign trips data.
        filter_since (str): Date string in 'YYYY-MM-DD' format to filter trips data.

    Returns:
        pd.DataFrame: DataFrame with foreign trips analysis added.
    """
    # Load the foreign trips data
    trips_df = pd.read_csv(trips_csv, encoding='utf-8')

    # Filter out rows where "EndDate" is empty or before the specified filter date
    trips_df = trips_df[trips_df['EndDate'].notna() & (trips_df['EndDate'] >= filter_since)]

    # Create a column Value for each trip, summing 'Donor_Value_1' to 'Donor_Value_5' where they are not NaN.
    trips_df['Value'] = trips_df[['Donor_Value_1', 'Donor_Value_2', 'Donor_Value_3', 'Donor_Value_4', 'Donor_Value_5']].sum(axis=1, min_count=1)
    trips_df['Value'] = trips_df['Value'].fillna(0)

    # Group by MP and sum the trip values
    trips_summary = trips_df.groupby('MNIS ID')['Value'].sum().reset_index()

    # Rename the columns for clarity
    trips_summary.rename(columns={'Value': 'TotalTripsValue'}, inplace=True)

    # For each MP in trips_summary, find their entry with the highest value, and copy over the fields 'Value', 'PaymentDescription', and 'DonorName'
    trips_summary[['max_trip_value', 'max_trip_description', 'max_trip_donor']] = trips_summary.apply(
        lambda row: pd.Series(
            trips_df[trips_df['MNIS ID'] == row['MNIS ID']].nlargest(1, 'Value')[['Value', 'PaymentDescription', 'DonorName']].values.flatten()
        ), axis=1
    )

    # Add a new column to trips_summary for the number of trips with a value >= 500, leaving a value of 0 if no trip data is available or none >= 500.
    expensive_trips = trips_df[trips_df['Value'] >= 500].groupby('MNIS ID')['Value'].count().reset_index()
    expensive_trips.rename(columns={'Value': 'expensive_trips_count'}, inplace=True)
    # Merge the expensive_trips count into trips_summary
    trips_summary = trips_summary.merge(expensive_trips, on='MNIS ID', how='left')
    # Fill NaN values with 0 for expensive_trips_count
    trips_summary['expensive_trips_count'] = trips_summary['expensive_trips_count'].fillna(0)

    # Add the data from trips summary to the main DataFrame as values, not a
def mp_infobox_html(r):
    """
    Generate an HTML infobox for an MP.

    Args:
        r (dict): A dictionary containing MP data.

    Returns:
        str: An HTML string representing the MP's infobox.
    """

    # Create a table with the MP's thumbnail on the left side and the MP's basic info on the right, inside <p> tags.
    thumbnail = f'<img src="{r["thumbnail"]}" alt="{r["name"]} thumbnail" style="width: 100px; height: auto;">' if r['thumbnail'] else ''
    basic_info = r['Basic Info']
    infobox_content = f"""
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="width: 120px; vertical-align: top;">{thumbnail}</td>
                <td style="vertical-align: top;">{basic_info}</td>
            </tr>
        </table>
    """

    specific_analyses = ['Property', 'Hospitality', 'Other', 'Outside Earnings']
    analyses_dict = {}
    for analysis in specific_analyses:
        analyses_dict[analysis] = (r[f'{analysis} Analysis'], r[f'{analysis} Score'])

    # Turn the analyses_dict into a list of analyses, ordered by score descending.
    analyses_list = sorted(analyses_dict.items(), key=lambda x: x[1][1], reverse=True)
    #analyses_text_list = [x[1][0] for x in analyses_list]

    # Within <p align="right">, add the analyses in descending order of score.
    #for x in analyses_text_list:
    for a in analyses_list:
        text_content = a[1][0]
        if len(text_content) > 0:
        #if len(x) > 0:
            text_content = text_content.replace("\n", "<br>\n")
            analysis_content = f"<p align='left'>{text_content}</p>"
            #y = x.replace("\n", "<br>\n")
            #z = f"<p align='left'>{y}</p>"
            if a[0] == 'Property':
                analysis_content = f"<p align='left'><strong>Housing and Accommodation:</strong> {analysis_content}</p>"
            elif a[0] == 'Hospitality':
                analysis_content = f"<p align='left'><strong>Hospitality and Gifts:</strong> {analysis_content}</p>"
            elif a[0] == 'Other':
                analysis_content = f"<p align='left'><strong>Other Information:</strong> {analysis_content}</p>"
            elif a[0] == 'Outside Earnings':
                analysis_content = f"<p align='left'><strong>Outside Earnings:</strong> {analysis_content}</p>"
            infobox_content += "\n"
            infobox_content += analysis_content

    # Add a split-out for lobbying analysis, if it exists. Requires a rewrite but pushing for quick preview.
    if r['DonorCategoriesCount'] > 0:
        infobox_content = infobox_content.replace("He!She has declared gifts or hospitality from",
                                "</p><p align='left'><strong>Lobbying:</strong></p>\n<p align='left'>He!She has declared gifts or hospitality from")

     # Add links to the MP's IPSA and parliament pages for expenses and interests.
    infobox_content += f"""
    <p align="right">You can get more detail from IPSA or Parliament about his!her
    <a href="https://www.theipsa.org.uk/mp-staffing-business-costs/your-mp/x/{r['mp_id']}" target="_blank">expenses</a> and
    <a href="https://members.parliament.uk/member/{r['mp_id']}/registeredinterests" target="_blank">financial interests</a>.</p>
    """

    infobox_content = his_her_pronoun(infobox_content, r['gender'])

    return infobox_content

def his_her_pronoun(text, gender):
    """
    Replace things like "his!her" with "his" or "her" based on the gender
    Args:
        text (str): The text to process
        gender (str): Gender, as M or F
    Returns:
        str: The text with pronouns replaced
    """

    if gender == 'M':
        text = text.replace("his!her", "his")
        text = text.replace("His!Her", "His")
        text = text.replace("he!she", "he")
        text = text.replace("He!She", "He")
        text = text.replace("him!her", "him")
        text = text.replace("Him!Her", "Him")
    else:
        text = text.replace("his!her", "her")
        text = text.replace("His!Her", "Her")
        text = text.replace("he!she", "she")
        text = text.replace("He!She", "She")
        text = text.replace("him!her", "her")
        text = text.replace("Him!Her", "Her")

    return text




if __name__ == "__main__":
    # Run from mp_collect directory
    import os

    # Set the working directory to the parent directory of the parent directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Add personal analysis to the MP data summary
    add_personal_analysis()