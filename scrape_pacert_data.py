import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_pacert_data(base_url):
    """
    Scrapes credential data from the PACERT Board website.

    Args:
        base_url (str): The base URL of the credential search page.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped data.
    """
    all_records = []
    page = 0
    while True:
        url = f"{base_url}&page={page}"
        print(f"Scraping page: {page + 1} ({url})")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page + 1}: {e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all the main table rows
        main_rows = soup.find_all('tr', class_="")

        if not main_rows:
            print("No more data found. Exiting.")
            break

        for row in main_rows:
            name_td = row.find('td', class_='views-field-nothing')
            location_td = row.find('td', class_='views-field-nothing-1')

            name = name_td.get_text(strip=True) if name_td else "N/A"
            location = location_td.get_text(strip=True) if location_td else "N/A"

            credential_table = row.find('table', class_='views-view-table')
            if credential_table:
                credential_rows = credential_table.find('tbody').find_all('tr')
                for cred_row in credential_rows:
                    cred_type = cred_row.find('td', class_='views-field-type').get_text(strip=True) if cred_row.find('td', class_='views-field-type') else "N/A"
                    cred_number = cred_row.find('td', class_='views-field-field-note').get_text(strip=True) if cred_row.find('td', class_='views-field-field-note') else "N/A"
                    issue_date = cred_row.find('td', class_='views-field-start-date').get_text(strip=True) if cred_row.find('td', class_='views-field-start-date') else "N/A"
                    expire_date = cred_row.find('td', class_='views-field-expire-date').get_text(strip=True) if cred_row.find('td', class_='views-field-expire-date') else "N/A"
                    status = cred_row.find('td', class_='views-field-membership-state').get_text(strip=True) if cred_row.find('td', class_='views-field-membership-state') else "N/A"

                    all_records.append({
                        'Name': name,
                        'Location': location,
                        'Credential Type': cred_type,
                        'Credential Number': cred_number,
                        'Issue Date': issue_date,
                        'Expire Date': expire_date,
                        'Status': status
                    })
            else:
                # If no credential table is found for a person, add them with N/A for credential details
                all_records.append({
                    'Name': name,
                    'Location': location,
                    'Credential Type': "N/A",
                    'Credential Number': "N/A",
                    'Issue Date': "N/A",
                    'Expire Date': "N/A",
                    'Status': "N/A"
                })

        # Check for the "Last »" link to determine if there are more pages
        next_page_link = soup.find('li', class_='pager__item--next')
        if not next_page_link:
            print("Reached the last page or no next page link found.")
            break

        page += 1
        time.sleep(1) # Be polite and avoid overwhelming the server

    return pd.DataFrame(all_records)

if __name__ == "__main__":
    initial_url = "https://www.pacertboard.org/credential-search?type=crs&status=active"
    df = scrape_pacert_data(initial_url)

    if not df.empty:
        # Save the data to a CSV file
        df.to_csv('pacert_credentials_crs_active.csv', index=False)
        print("\nData scraped successfully and saved to 'pacert_credentials_crs_active.csv'")
        print(df.head())
    else:
        print("\nNo data was scraped.")