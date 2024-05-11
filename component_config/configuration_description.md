##### Retino API token
API token can be found in settings of your Retino account at [https://app.retino.io/settings/api-v2/api/](https://app.retino.io/settings/api-v2/api/).
##### Incremental update
Downloading all data can take a long time. If you want to download only new data, you can enable incremental update. In this case, the component will download only tickets that were created or updated since the last download. (Incremental update is working only for tickets, other data are downloaded always in full.)
##### Data Selection
Select only the data you really need. You probably don't need to update all the data Retino provides every time. Tickets are important, but settings tables (users, tags, statuses, etc.) are probably not changing so often. Update your data selection according to your needs:
* **all data** - download all data (tickets and settings tables)
* **only tickets** - download only tickets
* **other resources** - download only settings tables (users, tags, statuses, etc.)
##### Default language
Default language is used only for better readability of the downloaded data. It doesn't affect the data itself and all the translations are downloaded to language tables anyway.