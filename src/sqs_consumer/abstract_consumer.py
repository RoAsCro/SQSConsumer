import logging
import os
import sys
import threading
from abc import ABC, abstractmethod

import boto3
from flask import Blueprint, Flask


class AbstractConsumer(ABC):
    def __init__(self):
        self.bg_thread = None
        default_region = "us-east-1"
        # Environment variables
        self.queue = os.getenv("QUEUE")
        self.aws_region = os.getenv("AWS_REGION")
        if self.aws_region is None:
            self.aws_region = default_region
        self.access_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.exception = Exception
        self.sqs = boto3.client("sqs",
                               region_name=self.aws_region,
                               aws_access_key_id=self.access_id,
                               aws_secret_access_key=self.access_key
                               )
        self.error_logger = logging.getLogger()
        self.error_logger.addHandler(logging.StreamHandler(sys.stdout))
        self.error_logger.setLevel(logging.ERROR)

        self.info_logger = logging.getLogger()
        self.info_logger.addHandler(logging.StreamHandler(sys.stdout))
        self.info_logger.setLevel(logging.INFO)

    running = False
    router = Blueprint("messages", __name__, url_prefix="/queue_1")

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

    @abstractmethod
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
        self.bg_thread = self.background_thread()
        health_checker = Flask(__name__)
        health_checker.register_blueprint(self.router)
        return health_checker