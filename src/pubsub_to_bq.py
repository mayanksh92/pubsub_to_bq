import argparse
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import logging


bq_client = bigquery.Client()

def write_to_bq(msg):
    '''
    :param msg: msg from pubsub
    '''
    # Prepare rows to be inserted

    rows_to_insert = [
        {u"txt": msg}
    ]

    errors = bq_client.insert_rows_json(bq_table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")


def callback(message):
    msg = message.data.decode('utf-8')
    print(f"Received message: {msg}")
    write_to_bq(msg)
    message.ack()


def read_from_pubsub(project_id:str,topic_id:str, subscription_id:str, bq_table_id:str)-> str:
    '''
    :param project_id: enter gcp project id
    :param topic_id: enter pubsub topic id
    :param subscription_id: enter pubsub subscription id
    :param bq_table_id: enter bq_table_id
    :return: retrun msg
    '''

    subs_client = pubsub_v1.SubscriberClient()
    subs_path = subs_client.subscription_path(project_id, subscription_id)

# Function to receive messages from the Pub/Sub subscription

    try:
        streaming_pull_future = subs_client.subscribe(subs_path, callback=callback)
        print(f"Listening for messages on {subs_path}..\n")
        # Keep the main thread alive while waiting for messages
        with subs_client:
            try:
                streaming_pull_future.result()
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
    except Exception as e:
        print(f"An error occurred while receiving messages: {e}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", dest="project_id")
    parser.add_argument("--topic_id", dest="topic_id")
    parser.add_argument("--subscription_id", dest="subscription_id")
    parser.add_argument("--bq_table_id", dest="bq_table_id")

    arg = parser.parse_args()
    project_id = arg.project_id
    topic_id = arg.topic_id
    bq_table_id = arg.bq_table_id
    subscription_id = arg.subscription_id

    # logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    #                     datefmt='%Y-%m-%d:%H:%M:%S',
    #                     level=logging.DEBUG)
    logging.info('Starting to read msg from PubSub Topic....')

    read_from_pubsub(project_id, topic_id, subscription_id, bq_table_id)
