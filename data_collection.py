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
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
    headers = {'User-Agent': user_agent}

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

    return http, headers

def get_park_units():
    http, headers = get_connection_info()
    park_units_url = 'https://irmaservices.nps.gov/v2/rest/unit/designations'
    park_unit_exceptions = {'DENG':'DENA', 'GAAG':'GAAR', 'GLBG':'GLBA', 'GRDG':'GRSA', 'KATG':'KATM', 'LACG':'LACL', 'WRSG':'WRST'}
    
    national_parks = {}
    r = http.get(park_units_url, headers=headers)
    soup = BeautifulSoup(r.text, 'xml')

    for unit_designation in soup.find_all('UnitDesignation'):
        if unit_designation.find("Code").text == 'NP':
            units = unit_designation.find("Units")
            for value in units.find_all('Value'):
                raw_code = value.find("Code").text
                name = value.find("Name").text
                code = park_unit_exceptions.get(raw_code, raw_code)
                national_parks[code] = name

    # Manually add New River Gorge, if it hasn't been added yet
    if 'NERI' not in national_parks.keys():
        national_parks['NERI'] = 'New River Gorge'

    park_units_df = pd.DataFrame.from_dict(national_parks, orient='index', columns=['name'])
    park_units_df.index.name = 'code'
    park_units_df.sort_index(inplace=True)

    return park_units_df

def get_park_visits(park_units_df):
    http, headers = get_connection_info()

    # park_visits_homepage = 'https://irma.nps.gov/STATS/'
    park_visits_domain = 'https://irma.nps.gov'
    park_visits_url = '/STATS/SSRSReports/Park%20Specific%20Reports/Recreation%20Visitors%20By%20Month%20(1979%20-%20Last%20Calendar%20Year)'
    park_visits_qs = '?Park='
    park_visits_df = pd.DataFrame()
    target_table_min = 10

    print('Processing:', end=" ")
    for park_code in park_units_df.index:
        print(park_code, end=", ")
        park_visits_request = park_visits_domain + park_visits_url + park_visits_qs + park_code
        r = http.get(park_visits_request, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, 'html.parser')
        park_visits_iframe = soup.find('iframe').attrs['src']

        park_visits_request = park_visits_domain + park_visits_iframe
        r = http.get(park_visits_request, headers=headers, timeout=5)

        dfs = pd.read_html(r.text, match="Year", skiprows=1)
        for df in dfs:
            if len(df) > target_table_min: one_park_df = df
        
        new_header = one_park_df.iloc[0] #grab the first row for the header
        one_park_df = one_park_df[1:] #take the data less the header row
        one_park_df.columns = new_header #set the header row as the df header
    
        one_park_df = one_park_df.fillna(0)
        if 'Total' in one_park_df.columns:
            one_park_df.drop('Total', axis=1, inplace=True)

        one_park_df.set_index('Year', inplace=True)
        one_park_srs = one_park_df.stack()

        park_visits_df[park_code] = one_park_srs

    park_visits_df = park_visits_df.fillna(0)
    park_visits_df.index.names = ['Year', 'Month']

    return park_visits_df

def get_park_data(park_units_df):
    http, headers = get_connection_info()

    park_data_url = 'https://en.wikipedia.org/wiki/List_of_national_parks_of_the_United_States'    
    r = http.get(park_data_url, headers=headers)
    soup = BeautifulSoup(r.text, "xml")
    # print(soup)

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
    # print(nps_names)
    for index, row in park_data_df.iterrows():
        # print(park_data_df.loc[index,'Name'])
        matching_name, matching_ratio = process.extractOne(park_data_df.loc[index,'Name'], nps_names)
        # print(matching_name, matching_ratio)
        # print(park_units_df[park_units_df['name'] == matching_name].index.tolist()[0])
        park_data_df.loc[index,'Code'] = park_units_df[park_units_df['name'] == matching_name].index.tolist()[0]
    
    # Parse state from Location into new column; update Location to only lat/long coordinates
    park_data_df['State'] = park_data_df['Location'].apply(lambda x: re.split('[^a-zA-Z\s\.]', x)[0].replace('.mw', ''))
    park_data_df['Location'] = park_data_df['Location'].apply(lambda x: x.rpartition('/')[2].strip())

    # Remove footnotes from established dates
    park_data_df['Established'] = park_data_df['Established'].apply(lambda x: re.sub('\[\d*?\]', '', x).strip())

    # Clean Acres column
    park_data_df['Acres'] = park_data_df['Acres'].apply(lambda x: re.split('[^\d\,\.]', x)[0])

    park_data_df = park_data_df[['Code', 'Name', 'State', 'Location', 'Established', 'Acres', 'Description']]

    park_data_df.set_index('Code', inplace=True)
    return park_data_df
    # print(park_data_df.head())