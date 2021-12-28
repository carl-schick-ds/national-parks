# pip install thefuzz
# pip install python-Levenshtein

# Import needed libraries.  Unless noted above, all libraries are available in the baseline conda environment.
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from thefuzz import process
import pandas as pd
import requests
import re


def get_connection_info():
    # Configure HTTP components for web browser immitation and retry sessions

    # User Agent makes us look like a web browser to avoid connection denials
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
    headers = {'User-Agent': user_agent}

    # Setup retry strategy and attach it to the http adapter
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    # Return the http adapater (with retries configured) and the headers variable with a user agent that looks like a browser
    return http, headers

def get_park_units():
    # Get connection info
    http, headers = get_connection_info()
    park_units_url = 'https://irmaservices.nps.gov/v2/rest/unit/designations'

    # Some park unit codes obtained from IRMA don't match the park units for visit retrieval.  The exceptions are mapped in this dictionary so they can be corrected on import.
    park_unit_exceptions = {'DENG':'DENA', 'GAAG':'GAAR', 'GLBG':'GLBA', 'GRDG':'GRSA', 'KATG':'KATM', 'LACG':'LACL', 'WRSG':'WRST'}
    
    # Make a call to get the list of units
    national_parks = {}
    r = http.get(park_units_url, headers=headers)
    soup = BeautifulSoup(r.text, 'xml')

    # Use Beautiful Soup to find the National Park units (Code = NP)
    for unit_designation in soup.find_all('UnitDesignation'):
        if unit_designation.find("Code").text == 'NP':
            units = unit_designation.find("Units")
            # For each National Park found, extract the unit code and the park's name
            for value in units.find_all('Value'):
                raw_code = value.find("Code").text
                name = value.find("Name").text
                code = park_unit_exceptions.get(raw_code, raw_code)
                national_parks[code] = name

    # Manually add New River Gorge, if it hasn't been added yet
    if 'NERI' not in national_parks.keys():
        national_parks['NERI'] = 'New River Gorge'

    # Create a dataframe with the extracted park units
    park_units_df = pd.DataFrame.from_dict(national_parks, orient='index', columns=['name'])
    park_units_df.index.name = 'code'
    park_units_df.sort_index(inplace=True)

    # Return the park units dataframe
    return park_units_df

def get_park_visits(park_units_df):
    # Get connection info
    http, headers = get_connection_info()

    # Base page for manual retrieval is at URL 'https://irma.nps.gov/STATS/'
    # Setup URL and Query String for coded retrieval
    park_visits_domain = 'https://irma.nps.gov'
    park_visits_url = '/STATS/SSRSReports/Park%20Specific%20Reports/Recreation%20Visitors%20By%20Month%20(1979%20-%20Last%20Calendar%20Year)'
    park_visits_qs = '?Park='

    # Create an empty dataframe to hold the results
    # When the data is scraped, the HTML has many duplicate tables with like headings.  We find the right table my looking for the one that has multiple rows.
    # The target_table_min literal identifies the number of rows a table must have in order for it to be considered the valid table we are looking for.
    park_visits_df = pd.DataFrame()
    target_table_min = 10

    # Loop through each park unit and query for the visits
    print('Processing:', end=" ")
    for park_code in park_units_df.index:
        print(park_code, end=", ")

        # The first call will get the SSRS wrapper
        park_visits_request = park_visits_domain + park_visits_url + park_visits_qs + park_code
        r = http.get(park_visits_request, headers=headers, timeout=5)

        # Use Beautiful Soup to extract the source (src) of the iframe that contains the actual data
        soup = BeautifulSoup(r.text, 'html.parser')
        park_visits_iframe = soup.find('iframe').attrs['src']

        # Make a second call to the iframe's src URL to get the actual data
        park_visits_request = park_visits_domain + park_visits_iframe
        r = http.get(park_visits_request, headers=headers, timeout=5)

        # Use pandas to read in the html and find the target table
        dfs = pd.read_html(r.text, match="Year", skiprows=1)
        for df in dfs:
            if len(df) > target_table_min: one_park_df = df
        
        # Extract the data for our park
        new_header = one_park_df.iloc[0] #grab the first row for the header
        one_park_df = one_park_df[1:] #take the data less the header row
        one_park_df.columns = new_header #set the header row as the df header
    
        # Fill nan values with zero (0) and drop the Total column if it exists
        one_park_df = one_park_df.fillna(0)
        if 'Total' in one_park_df.columns:
            one_park_df.drop('Total', axis=1, inplace=True)

        # Set the index to the Year and Stack the data into a single column
        # Each park will be a single column in the final dataframe
        one_park_df.set_index('Year', inplace=True)
        one_park_srs = one_park_df.stack()

        # Add the series for this park into the final dataframe
        park_visits_df[park_code] = one_park_srs

    # After all parks of been collected as columns, perform a final fill of nan to zero(0) and create a multi-index for Year and Month
    park_visits_df = park_visits_df.fillna(0)
    park_visits_df.index.names = ['Year', 'Month']

    # Return the park visits dataframe
    return park_visits_df

def get_park_data(park_units_df):
    # Get connection info
    http, headers = get_connection_info()

    # Grab the Wikipedia page with national park data
    park_data_url = 'https://en.wikipedia.org/wiki/List_of_national_parks_of_the_United_States'    
    r = http.get(park_data_url, headers=headers)

    # Extract the table by looking for the "Date established as park" literal string
    dfs = pd.read_html(r.text, match="Date established as park")
    park_data_df = dfs[0]

    # Strip years and footnotes from columns
    for column_name in park_data_df.columns.values:
        if '(' in column_name or '[' in column_name:
            new_column_name = re.sub("\(.*?\)","", column_name)
            new_column_name = re.sub("\[.*?\]","", new_column_name)
            new_column_name = new_column_name.strip()
            park_data_df.rename(columns={column_name: new_column_name}, inplace=True)
 
    # Rename some columns and drop unneeded columns
    park_data_df.rename(columns={'Date established as park': 'Established', 'Area': 'Acres'}, inplace=True)
    park_data_df.drop(columns=['Image', 'Recreation visitors'], inplace=True)

    # Remove asterisks from the park names
    park_data_df['Name'] = park_data_df['Name'].str.replace('*', '', regex=True).str.strip()

    # Add each park's unit code
    # We will need fuzzy string matching logic to match the name from Wikipedia with the name from NPS
    nps_names = park_units_df['name'].to_list()
    for index, row in park_data_df.iterrows():
        matching_name, _ = process.extractOne(park_data_df.loc[index,'Name'], nps_names)
        park_data_df.loc[index,'Code'] = park_units_df[park_units_df['name'] == matching_name].index.tolist()[0]
    
    # Parse state from Location into new column; update Location to only lat/long coordinates
    park_data_df['State'] = park_data_df['Location'].apply(lambda x: re.split('[^a-zA-Z\s\.]', x)[0].replace('.mw', ''))
    park_data_df['Location'] = park_data_df['Location'].apply(lambda x: x.rpartition('/')[2].strip())

    # Remove footnotes from established dates
    park_data_df['Established'] = park_data_df['Established'].apply(lambda x: re.sub('\[\d*?\]', '', x).strip())

    # Clean Acres column
    park_data_df['Acres'] = park_data_df['Acres'].apply(lambda x: re.split('[^\d\,\.]', x)[0])

    # Reorder the dataframe columns and set the index
    park_data_df = park_data_df[['Code', 'Name', 'State', 'Location', 'Established', 'Acres', 'Description']]
    park_data_df.set_index('Code', inplace=True)

    # Return the park data dataframe
    return park_data_df