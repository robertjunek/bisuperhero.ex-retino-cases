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

        increment = False

        # values for KEY_DATA_TABLES are: "all data", "only tickets", "other resources"
        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "other resources":
            logging.info("Downloading settings tables")

            # missing processing for endpoint "shipping-routes" as it is not available in the API

            # processing custom-fields
            endpoint = "custom-fields"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_custom_fields(data, endpoint)

            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing product-custom-fields
            endpoint = "product-custom-fields"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_product_custom_fields(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing refund-accounts
            endpoint = "refund-accounts"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # process the data

                self.process_refund_accounts(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing states
            endpoint = "states"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # process the data

                self.process_states(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing tags
            endpoint = "tags"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # process the data

                self.process_tags(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing types
            endpoint = "types"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # process the data

                self.process_types(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing users
            endpoint = "users"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # process the data

                self.process_users(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "only tickets":
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

    def process_product_custom_fields(self, data, endpoint):
        fields_output_path = os.path.join(self.data_folder, f'{endpoint}_fields.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')
        options_output_path = os.path.join(self.data_folder, f'{endpoint}_options.csv')
        option_labels_output_path = os.path.join(self.data_folder, f'{endpoint}_option_labels.csv')

        # Handling fields file
        with open(fields_output_path, 'w', newline='') as fields_file:
            field_writer = csv.writer(fields_file)
            field_writer.writerow(['id', 'type', 'position'])
            for field in data:
                field_writer.writerow([field['id'], field['type'], field['position']])

        # Handling names file
        with open(names_output_path, 'w', newline='') as names_file:
            name_writer = csv.writer(names_file)
            name_writer.writerow(['field_id', 'language_code', 'value'])
            for field in data:
                for lang, name in field['name'].items():
                    name_writer.writerow([field['id'], lang, name])

        # Handling options file
        with open(options_output_path, 'w', newline='') as options_file:
            option_writer = csv.writer(options_file)
            option_writer.writerow(['id', 'field_id'])
            for field in data:
                for option in field.get('options', []):
                    option_writer.writerow([option['id'], field['id']])

        # Handling option labels file
        with open(option_labels_output_path, 'w', newline='') as option_labels_file:
            option_label_writer = csv.writer(option_labels_file)
            option_label_writer.writerow(['option_id', 'language_code', 'value'])
            for field in data:
                for option in field.get('options', []):
                    for lang, label in option['label'].items():
                        option_label_writer.writerow([option['id'], lang, label])

        # Generate manifest files for each CSV
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

    def process_refund_accounts(self, data, endpoint):
        output_path = os.path.join(self.data_folder, f'{endpoint}.csv')

        # Opening the CSV file
        with open(output_path, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write the header row
            writer.writerow(['id', 'name', 'bank_account', 'currency', 'due_date'])

            # Iterate over the results and write each row
            for account in data:
                writer.writerow([
                    account['id'],
                    account['name'],
                    account['bank_account'],
                    account['currency'],
                    account['due_date']
                ])

        # Generate a manifest file for the CSV
        self.create_manifest(output_path, ['id'])

    def process_states(self, data, endpoint):
        states_output_path = os.path.join(self.data_folder, f'{endpoint}.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')

        # Handling states file
        with open(states_output_path, 'w', newline='') as states_file:
            state_writer = csv.writer(states_file)
            state_writer.writerow(['id'])

            for state in data:
                state_writer.writerow([state['id']])

        # Handling names file
        with open(names_output_path, 'w', newline='') as names_file:
            name_writer = csv.writer(names_file)
            name_writer.writerow(['state_id', 'language_code', 'name'])

            for state in data:
                for lang, name in state.get('name', {}).items():
                    name_writer.writerow([state['id'], lang, name])

        # Generate manifest files for each CSV
        self.create_manifest(states_output_path, ['id'])
        self.create_manifest(names_output_path, ['state_id', 'language_code'])

    def process_tags(self, data, endpoint):
        tags_output_path = os.path.join(self.data_folder, f'{endpoint}.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')

        # Handling tags file
        with open(tags_output_path, 'w', newline='') as tags_file:
            tag_writer = csv.writer(tags_file)
            tag_writer.writerow(['id', 'fgcolor', 'bgcolor'])

            for tag in data:
                tag_writer.writerow([tag['id'], tag['fgcolor'], tag['bgcolor']])

        # Handling names file
        with open(names_output_path, 'w', newline='') as names_file:
            name_writer = csv.writer(names_file)
            name_writer.writerow(['tag_id', 'language_code', 'name'])

            for tag in data:
                for lang, name in tag['name'].items():
                    name_writer.writerow([tag['id'], lang, name])

        # Generate manifest files for each CSV
        self.create_manifest(tags_output_path, ['id'])
        self.create_manifest(names_output_path, ['tag_id', 'language_code'])

    def process_types(self, data, endpoint):
        types_output_path = os.path.join(self.data_folder, f'{endpoint}.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')

        # Handling types file
        with open(types_output_path, 'w', newline='') as types_file:
            type_writer = csv.writer(types_file)
            type_writer.writerow(['id'])

            for type_ in data:
                type_writer.writerow([type_['id']])

        # Handling names file
        with open(names_output_path, 'w', newline='') as names_file:
            name_writer = csv.writer(names_file)
            name_writer.writerow(['type_id', 'language_code', 'name'])

            for type_ in data:
                if 'name' in type_:
                    for lang, name in type_['name'].items():
                        name_writer.writerow([type_['id'], lang, name])

        # Generate manifest files for each CSV
        self.create_manifest(types_output_path, ['id'])
        self.create_manifest(names_output_path, ['type_id', 'language_code'])

    def process_users(self, data, endpoint):
        users_output_path = os.path.join(self.data_folder, f'{endpoint}.csv')

        # Handling users file
        with open(users_output_path, 'w', newline='') as users_file:
            user_writer = csv.writer(users_file)
            user_writer.writerow(['id', 'role', 'email', 'full_name',
                                  'phone_number', 'last_activity_at', 'date_joined'])

            for user in data:
                user_writer.writerow([
                    user['id'],
                    user['role'],
                    user['email'],
                    user['full_name'],
                    user['phone_number'],
                    user['last_activity_at'],
                    user['date_joined']
                ])

        # Generate manifest file for the CSV
        self.create_manifest(users_output_path, ['id'])


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
