"""
Template Component main class.

"""

import csv
import os
import logging
import requests
# from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_DATA_TABLES = "data_selection"
KEY_INCREMENTAL_UPDATE = "incremental_update"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_TOKEN, KEY_DATA_TABLES]


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """

        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Access parameters in data/config.json
        # values for KEY_DATA_TABLES are: "all data", "only tickets", "other resources"
        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "other resources":
            logging.info("Downloading settings tables")

            # list of endpoints to download
            endpoints = [
                "custom-fields",
                "product-custom-fields",
                "refund-accounts",
                # "shipping-routes",
                "states",
                "tags",
                "types",
                "users"
            ]

            # download data for each endpoint
            for endpoint in endpoints:
                logging.info(f"Downloading data for endpoint {endpoint}")
                try:
                    data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
                    self.save_data_csv(data, endpoint)
                except Exception as e:
                    logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")
                    continue

        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "only tickets":
            logging.info("=====================================")
            logging.info("Downloading tickets data")

    def save_data_csv(self, data, filename):
        """
        Function to save data to CSV file.
        To reduce memory usage, data is saved row by row.
        """
        # Assume 'data' is already a list of dictionaries, no need to call .json() or .get('results', [])
        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition(filename + '.csv', primary_key=['id'])

        # Check if the directory exists, if not, create it
        directory = os.path.dirname(table.full_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # path to the output file
        out_table_path = table.full_path

        # Write data to the output file from JSON response row by row
        with open(out_table_path, 'w', newline='') as csvfile:
            if data:  # Check if data is not empty
                writer = csv.writer(csvfile)
                if data:  # Additional check if data is not empty to avoid IndexError
                    writer.writerow(data[0].keys())  # headers
                for row in data:
                    writer.writerow(row.values())

    def get_retino_data(self, token, endpoint, increment=False, last_update=None):
        """
        Function to fetch all pages of data from the Retino API
        """
        # URL for the API
        url = f"https://app.retino.com/api/v2/{endpoint}"

        # Set headers
        headers = {"Authorization": f"Token {token}"}

        # Initialize data collection
        all_data = []

        # Start with the first page
        current_page = 1
        total_pages = 1  # Assume there is at least one page

        while current_page <= total_pages:
            # Get data from the API
            response = requests.get(url, headers=headers, params={'page': current_page})
            if response.status_code != 200:
                logging.error(f"Failed to get data from Retino API for endpoint {endpoint}. "
                              f"Returned status code: {response.status_code}")
                raise Exception(f"Failed to get data from Retino API for endpoint {endpoint}. "
                                f"Returned status code: {response.status_code}")

            # Convert response to JSON
            data = response.json()

            # Add the results to the all_data list
            all_data.extend(data.get('results', []))

            # Update total_pages and current_page
            total_pages = data.get('total_pages', 1)
            current_page += 1

        return all_data


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
