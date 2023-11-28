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

## Configuration

- Set up your Gmail API credentials and save them as `credentials.json` in the script directory.
- Update the MongoDB connection string in the `MongoClient` instantiation within the `send_emails_to_mongodb` function.

