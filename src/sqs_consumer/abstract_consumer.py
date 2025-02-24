import logging
import threading
from abc import ABC

import boto3
from flask import Blueprint, Flask

class AbstractConsumer(ABC):

    default_region = "us-east-1"
    env = {}
    # Environment variables
    queue = env.get("QUEUE")
    aws_region = env.get("AWS_REGION")
    if aws_region is None:
        aws_region = default_region
    access_id = env.get("AWS_ACCESS_KEY_ID")
    access_key = env.get("AWS_SECRET_ACCESS_KEY")

    exception = Exception
    router = Blueprint("messages", __name__, url_prefix="/queue_1")
    def __init__(self, env):
        self.env = env

    sqs = boto3.client("sqs",
                       region_name=aws_region,
                       aws_access_key_id=access_id,
                       aws_secret_access_key=access_key
                       )

    running = False


    def get_from_queue(self):
        response = self.sqs.receive_message(
            QueueUrl=self.queue,
            MaxNumberOfMessages=1,
            MessageAttributeNames=["All"],
            VisibilityTimeout=0,
            WaitTimeSeconds=20
        )

        if "Messages" not in response:
            return None

        message = response["Messages"][0]

        return message

    def send(self, message_to_send):
        pass

    def delete(self, message):
        receipt_handle = message["ReceiptHandle"]

        self.sqs.delete_message(
            QueueUrl=self.queue,
            ReceiptHandle=receipt_handle
        )

    def process(self):
        self.running = True
        while self.running:
            message = self.get_from_queue()
            if message:
                try:
                    self.send(message)
                except self.exception as ex:
                    logging.error(ex)
                    continue
                self.delete(message)

    def background_thread(self):
        thread = threading.Thread(target=self.process, daemon=True)
        thread.start()
        return thread



    @router.get("/health")
    def health_check(self):
        return 'Ok', 200

    def run(self):
        health_checker = Flask(__name__)
        health_checker.register_blueprint(self.router)
        return health_checker