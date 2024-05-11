import csv
import json
import os
import logging
import requests
import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException


# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_DATA_TABLES = "data_selection"
KEY_INCREMENTAL_UPDATE = "incremental_update"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_TOKEN, KEY_DATA_TABLES]

# check if exists file with a name of localhost.json in the same directory as the component
# if yes, set LOCALHOST_MODE to True
LOCALHOST_MODE = os.path.exists(os.path.join(os.path.dirname(__file__), 'localhost.json'))


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
        Main component function
        """

        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # set the data folder
        self.data_folder = self.tables_out_path

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
                self.process_refund_accounts(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            endpoint = "states"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_states(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing tags
            endpoint = "tags"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_tags(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing types
            endpoint = "types"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_types(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

            # processing users
            endpoint = "users"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                self.process_users(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

        if params.get(KEY_DATA_TABLES) == "all data" or params.get(KEY_DATA_TABLES) == "only tickets":
            logging.info("Downloading tickets data")

            # set incremental update flag for tickets only
            increment = params.get(KEY_INCREMENTAL_UPDATE, False)

            endpoint = "tickets"
            logging.info(f"Downloading data for endpoint {endpoint}")
            try:
                data = self.get_retino_data(params.get(KEY_API_TOKEN), endpoint, increment)
                # self.process_tickets(data, endpoint)
            except Exception as e:
                logging.error(f"Error downloading data for endpoint {endpoint}: {str(e)}")

    def process_custom_fields(self, data, endpoint):
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
        """Creates a manifest file for a CSV file

        Args:
            csv_file_path (str): File path of the CSV file
            primary_keys (dict): List of primary keys
        """
        manifest_path = f"{csv_file_path}.manifest"
        manifest_data = {
            "primary_key": primary_keys,
            "incremental": False,
            "columns": []
        }
        with open(manifest_path, 'w') as manifest_file:
            json.dump(manifest_data, manifest_file)

    def get_retino_data(self, token, endpoint, increment=False, page_size=100):
        """
        Fetches all pages of data from the Retino API handling pagination and potential network errors gracefully.
        """
        url = f"https://app.retino.com/api/v2/{endpoint}"
        headers = {"Authorization": f"Token {token}"}
        all_data = []
        current_page = 1
        params = {"page": current_page, "page_size": page_size}

        # if endpoint is tickets and incremental update is enabled, add last update timestamp to params
        if endpoint == "tickets" and increment:
            previous_state = self.get_state_file()
            previous_run = previous_state.get("lastTicketsUpdate", 0)
            previous_run = datetime.datetime.fromtimestamp(previous_run, datetime.timezone.utc)
            previous_run = previous_run.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            params["updated_at_from"] = previous_run
            params["page_size"] = 10

        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()  # Raises an HTTPError for bad responses
                data = response.json()
                all_data.extend(data.get('results', []))
                if current_page >= data.get('total_pages', 1):
                    break
                if current_page == 1:
                    logging.info(f"Total pages to process: {data.get('total_pages', 1)}")
                if LOCALHOST_MODE:
                    logging.info("Stopping after first page in localhost mode")
                    break
                current_page += 1
                params["page"] = current_page
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error when fetching data from {url}: {str(e)}")
                raise UserException(f"Failed to fetch data due to a network error: {str(e)}")

        # if endpoint was tickets, save current timestamp for incremental updates
        # 2 hours are subtracted to account for potential timezone differences
        if endpoint == "tickets":
            self.write_state_file({"lastTicketsUpdate":
                                   int((datetime.datetime.now() - datetime.timedelta(hours=2)).timestamp())})

        return all_data

    def process_refund_accounts(self, data, endpoint):
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
        types_output_path = os.path.join(self.data_folder, f'{endpoint}.csv')
        names_output_path = os.path.join(self.data_folder, f'{endpoint}_names.csv')

        # Handling types file
        with open(types_output_path, 'w', newline='') as types_file:
            type_writer = csv.writer(types_file)
            type_writer.writerow(['id', 'name'])

            for type_ in data:
                # Selecting name based on language preference
                name = self.select_name_by_preference(type_['name'])
                type_writer.writerow([type_['id'], name])

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

    def select_name_by_preference(self, names, lang="en"):
        """Settles the name based on the preferred language order

        Args:
            names (dict): The names in different languages

        Returns:
            str: The name in the preferred language
        """

        # Language preference order
        preferred_languages = [lang, 'en', 'cs']
        for lang in preferred_languages:
            if lang in names:
                return names[lang]

        # Fallback: If no preferred language is available, use the first available translation
        first_key = list(names.keys())[0]
        return f"{names[first_key]} ({first_key})"

    def process_users(self, data, endpoint):
        """Processes data from specific endpoint and saves it to CSV files

        Args:
            data (dict): The data to be sent to the endpoint
            endpoint (str): The URL of the endpoint
        """
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
