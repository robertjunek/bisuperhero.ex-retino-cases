import csv
import json
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

        # set the data folder
        self.data_folder = self.tables_out_path
        logging.info(f"Data folder: {self.data_folder}")

        # values for KEY_DATA_TABLES are: "all data", "only tickets", "other resources"
        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "other resources":
            logging.info("Downloading settings tables")

            # missing processing for endpoint "shipping-routes" as it is not available in the API

            # processing custom-fields
            endpoint = "custom-fields"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
                self.process_custom_fields(data, endpoint)

            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing product-custom-fields
            # endpoint = "product-custom-fields"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing refund-accounts
            # endpoint = "refund-accounts"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing states
            # endpoint = "states"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing tags
            # endpoint = "tags"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing types
            # endpoint = "types"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # # processing users
            # endpoint = "users"
            # logging.info(f"Downloading data for endpoint {endpoint}")
            # try:
            #     data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint)
            #     # process the data

            #     self.save_data_csv(data, endpoint)
            # except Exception as e:
            #     logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "only tickets":
            logging.info("=====================================")
            logging.info("Downloading tickets data")

    def process_custom_fields(self, data, endpoint):
        fields_output_path = os.path.join(self.data_folder, f'{endpoint}_fields.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')
        options_output_path = os.path.join(self.data_folder, f'{endpoint}_options.csv')
        option_labels_output_path = os.path.join(self.data_folder, f'{endpoint}_option_labels.csv')

        # Handle each file separately
        with open(fields_output_path, 'w', newline='') as fields_file:
            field_writer = csv.writer(fields_file)
            field_writer.writerow(['id', 'type', 'position'])
            for field in data:
                field_writer.writerow([field['id'], field['type'], field['position']])

        with open(names_output_path, 'w', newline='') as names_file:
            name_writer = csv.writer(names_file)
            name_writer.writerow(['field_id', 'language_code', 'value'])
            for field in data:
                for lang, name in field['name'].items():
                    name_writer.writerow([field['id'], lang, name])

        with open(options_output_path, 'w', newline='') as options_file:
            option_writer = csv.writer(options_file)
            option_writer.writerow(['id', 'field_id'])
            for field in data:
                for option in field.get('options', []):
                    option_writer.writerow([option['id'], field['id']])

        with open(option_labels_output_path, 'w', newline='') as option_labels_file:
            option_label_writer = csv.writer(option_labels_file)
            option_label_writer.writerow(['option_id', 'language_code', 'value'])
            for field in data:
                for option in field.get('options', []):
                    for lang, label in option['label'].items():
                        option_label_writer.writerow([option['id'], lang, label])

        # Creating manifest files
        self.create_manifest(fields_output_path, ['id'])
        self.create_manifest(names_output_path, ['field_id', 'language_code'])
        self.create_manifest(options_output_path, ['id', 'field_id'])
        self.create_manifest(option_labels_output_path, ['option_id', 'language_code'])

    def create_manifest(self, csv_file_path, primary_keys):
        manifest_path = f"{csv_file_path}.manifest"
        manifest_data = {
            "primary_key": primary_keys,
            "incremental": False,
            "columns": []
        }
        with open(manifest_path, 'w') as manifest_file:
            json.dump(manifest_data, manifest_file)

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
