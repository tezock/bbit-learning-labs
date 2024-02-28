# imoprt the consumer interface
from consumer_interface import mqConsumerInterface

# handle required imports
import pika
import os

# import sys to read from the command line
import sys

# import json for deserialization
import json

class mqConsumer(mqConsumerInterface):

    def __init__(self, exchange_name: str) -> None:
        # Save parameters to class variables
        self._exchange_name = exchange_name

        # parameters that may or may not be instantiated immediately.
        self._channel = None

        # Call setupRMQConnection
        self.setupRMQConnection()

        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        channel = connection.channel()

        # store the channel in the instance variable
        self._channel = channel

        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange=self._exchange_name)

        pass

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange

        self._channel.queue_bind(
        queue= queueName,
        routing_key= sys.argv[0],
        exchange=self._exchange_name,
        )

        pass

    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present

        self._channel.queue_declare(queue=queueName)

        # Set-up Callback function for receiving messages
        self._channel.basic_consume(
            queueName, self.on_message_callback, auto_ack=False
        )

        pass

    def on_message_callback(self, channel, method_frame, header_frame, body):
        # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(JsonMessageObject)

        # Acknowledge And Print Message

        pass

    def startConsuming(self) -> None:
        # Start consuming messages
        self._channel.start_consuming()
        pass

