## Building a Modern Uber Eats Data Pipeline
![Architecture](img/dg.png)


## Dependencies

- `google-auth`
- `google-auth-oauthlib`
- `google-auth-httplib2`
- `google-api-python-client`
- `pymongo`
- `schedule`

You can install the necessary dependencies via pip:

```bash
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client pymongo schedule
```
## Setup

1. Create a project on the [Google Cloud Platform Console](https://console.cloud.google.com/).
2. Enable the Gmail API for your project.
3. Download the client configuration file `credentials.json` and place it in the root directory of this script.
4. Setup a MongoDB Atlas cluster and obtain the connection string.

## Gmail Configuration Completion

Once the Gmail configuration is complete, let's face the challenge. It's unclear how Uber Eats email receipts look in different countries, but here is what mine looks like:

![Uber Eats Receipt](img/recu.png)

Ensure you have the same format; otherwise, you might encounter errors when attempting to retrieve Uber Eats emails. Also, if your Uber Eats emails are in English, replace the field names in French with their English equivalents in your receipts.

After launching the `getGmail.py` script, approximately 500 Uber Eats emails are retrieved. You can adjust this number. They are sent to MongoDB as documents resembling this:

![MongoDB Document](img/mongo.png)

The ultimate goal is to have a star schema model like this:

![Star Schema Model](img/etoiles.png)

## Setting Up Environment Variables

Navigate to the `models` folder, create a `.env` file, and fill in these values:
## Configuring connections
To configure connections to MongoDB and Snowflake, follow the steps below.
### MongoDB connection details

Add these details to your configuration file or environment:

```plaintext
MONGODB_URI=mongodb+srv://username:password@atlascluster.xxxxxx.mongodb.net/admin?retryWrites=true&w=majority

SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=xxxx.us-east-1
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=PUBLIC

Note: You can create free accounts on Snowflake and MongoDB Atlas to have ready-to-use servers.
```
## Testing Connections

Once set up, launch the `mongodb_connection.py` and `snowflake_connection.py` files to test the connections. At this stage, everything should be okay.

## Creating Tables

Create the dimension tables by executing `create_table_snowflake.py`. Check in Snowflake to see the three dimension tables and the fact table created.

## Populating Tables

Execute `model.py` to populate the fact and dimension tables, preparing your dimensional model for analysis. You can use Power BI, Tableau, or create views for dashboarding.

## Manual Execution

For manual execution, follow the steps above.

## Automatic Execution with Airflow

For automatic execution:

1. Navigate to the `airflow_docker` folder.
2. Launch a local server by executing: `docker-compose up --build -d`.
3. Once ready, go to `localhost:8080` with username/password `airflow`.
4. Before anything else, add connections for Snowflake and MongoDB in the Admin tab.
5. Start the main DAG. If everything is okay, you should see an output similar to this:

![Airflow Output](img/airflow.png)

