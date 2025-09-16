from robocorp import workitems
from robocorp.tasks import task

from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

http = HTTP()
json = JSON()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic.json"
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"


@task
def produce_traffic_data():
    """
    Produces traffic data work items
    """
    print("produce")
    http.download(url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json", 
                  target_file=TRAFFIC_JSON_FILE_PATH, 
                  overwrite=True
                  )
    
    traffic_data_table = load_traffic_data_as_table()
    table.write_table_to_csv(traffic_data_table, "output/traffic.csv")
    
    filtered_table = filter_and_sort_traffic_data(traffic_data_table)
    table.write_table_to_csv(filtered_table, "output/traffic_filtered.csv")

    latest_data = get_latest_data_by_country(filtered_table)
    latest_data_as_table = table.create_table(latest_data)                                  # not required - for practice only
    table.write_table_to_csv(latest_data_as_table, "output/traffic_latest_by_country.csv")  # not required - for practice only

    payloads = create_work_item_payload(latest_data)
    payload_as_table = table.create_table(payloads)                                  # not required - for practice only
    table.write_table_to_csv(payload_as_table, "output/payload.csv")                # not required - for practice only

    save_work_items_payloads(payloads)

def load_traffic_data_as_table():
    """Format and filter traffic data into a table"""
    json_data = json.load_json_from_file(TRAFFIC_JSON_FILE_PATH)
    
    # "value" indicates the node at which the data resides
    return table.create_table(json_data["value"])

def filter_and_sort_traffic_data(dataset):
    """Given a table, returns a filtered and sorted table"""

    max_rate = 5.0
    target_gender = "BTSX"

    table.filter_table_by_column(dataset, GENDER_KEY, "==", target_gender)
    table.filter_table_by_column(dataset, RATE_KEY, "<", max_rate)
    table.sort_table_by_column(dataset, YEAR_KEY, False)

    return dataset
    
def get_latest_data_by_country(dataset):
    """
    Given a table sorted by date, groups data by country and 
    selects the first record in each group
    """
    COUNTRY_KEY = "SpatialDim"
    data_grouped = table.group_table_by_column(dataset, COUNTRY_KEY)    # returns a list of tables (grouped by country)
    latest_data_by_country = []                                         # will hold the latest from each grouped country
    for group in data_grouped:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row)

    return latest_data_by_country

def create_work_item_payload(dataset):
    """
    Given a list of objects, extracts specific properties. Tracks these 
    properties and their values in a new list
    """
    payloads = []

    for data in dataset:
        payloads.append(dict(
            country = data[COUNTRY_KEY],
            year = data[YEAR_KEY],
            rate = data[RATE_KEY]
        ))
    
    return payloads

def save_work_items_payloads(payloads):
    """Given a list of payloads, creates work items of each"""

    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)  
