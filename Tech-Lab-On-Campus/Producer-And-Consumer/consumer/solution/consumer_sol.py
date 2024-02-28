# imoprt the consumer interface
from consumer_interface import mqConsumerInterface

# handle required imports
import pika
import os

class mqConsumer(mqConsumerInterface):

    

    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables
        self._binding_key = binding_key
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._channel = None
        self._connection = None

        # Call setupRMQConnection
        self.setupRMQConnection()

        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # put the connection as a class attribute
        self._connection = connection

        # Establish Channel
        channel = connection.channel()

        # put this channel as a class attriute
        self._channel = channel

        # Create Queue if not already present
        channel.queue_declare(queue=self._queue_name)

        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange=self._exchange_name)

        # Bind Binding Key to Queue on the exchange
        channel.queue_bind(
            queue= self._queue_name,
            routing_key= self._binding_key,
            exchange=self._exchange_name,
        )

        # Set-up Callback function for receiving messages
        channel.basic_consume(
            self._queue_name, self.on_message_callback, auto_ack=False
        )
        pass

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)
        print(body)

        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self._channel.start_consuming()

        pass
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        
        # Close Channel
        self._channel.close()

        # Close Connection
        self._connection.close()
        
        pass
