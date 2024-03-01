import pika
import os

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.channel = None
        self.connection = None
        self.setupRMQConnection()

        return

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue_name)

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange_name, routing_key=self.binding_key)

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(self.queue_name, self.on_message_callback,  auto_ack=False)
        return

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        self.channel.basic_ack(method_frame.delivery_tag, False)
        #Print message (The message is contained in the body parameter variable)
        print(body)

        return

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()
        return

    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")

        # Close Channel
        if self.channel:
            self.channel.close()
        # Close Connection
        if self.connection:
            self.connection.close()
        return
