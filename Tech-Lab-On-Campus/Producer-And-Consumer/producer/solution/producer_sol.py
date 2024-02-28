# implorts
import pika
import os

# parent class
from producer_interface import mqProducerInterface


class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self._routing_key = routing_key
        self._exchange_name = exchange_name
        self._channel = None
        self._exchange = None
        self._connection  = None
        # call setupRMQConnection
        self.setupRMQConnection()

        pass


    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        self._connection = connection
        # Establish Channel
        channel = connection.channel() 
        self._channel = channel
        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange=self._exchange_name)
        self._exchange = exchange
        pass

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self._channel.basic_publish(
        exchange=self._exchange_name,
        routing_key=self._routing_key,
        body=message,
)
        # Close Channel
        self._channel.close()
        # Close Connection
        self._connection.close()
        pass
