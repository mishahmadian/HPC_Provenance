# -*- coding: utf-8 -*-
"""
    RabbitMQ Interface: Send/publish the collected logs to
    the Monitoring server.

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties, exceptions
from schedConfig import SchedConfig, ConfigReadExcetion
from threading import Thread, Event
from typing import Callable
from enum import Enum

class SchedConnection:
    def __init__(self, is_rpc=True):

        try:
            self.__config = SchedConfig()
            self.__server = self.__config.getServer()
            self.__port = self.__config.getPort()
            self.__username = self.__config.getUsername()
            self.__password = self.__config.getPassword()
            self.__Vhost = self.__config.getVhost()
            self.__rpc_queue = self.__config.getRPC_queue()

        except ConfigReadExcetion as confExp:
            raise CommunicationExp(confExp.getMessage(), CommunicationExp.Type.AMQP_SCHED__CONFIG)

        # Create Credentials with username/password
        self.__credentials = PlainCredentials(self.__username, self.__password)
        # Setup the connection parameters
        self.__params = ConnectionParameters(self.__server,
                                                self.__port,
                                                self.__Vhost,
                                                self.__credentials)
        # Establish a Connection
        self.__conn = self.__openConnection()
        # Open a Channel
        self.__channel = self.__openChannel(self.__conn)

        # Initialize RPC Server
        self.__rpc_callback_func = None
        if is_rpc:
            # Prepare the channel for RPC Requests
            self.__channel.queue_declare(queue=self.__rpc_queue)
            # Purge all the messages in this queue to avoid any process freeze
            self.__channel.queue_purge(queue=self.__rpc_queue)


    # Establish a Connection to RabbitMQ server
    def __openConnection(self):
        try:
            return BlockingConnection(self.__params)

        except (exceptions.ConnectionClosed, exceptions.AMQPConnectionError,
                exceptions.AMQPError) as amqExp:
            raise CommunicationExp("Connection was not established. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_SCHED_CONN)

    # Open a channel on current connection to RabbitMQ server
    def __openChannel(self, conn):
        try:
            return conn.channel()

        except(exceptions.AMQPChannelError, exceptions.AMQPError) as amqExp:
            raise CommunicationExp("couldn't open a Channel. {})".format(amqExp),
                                    CommunicationExp.Type.AMQP_SCHED_CHANL)
    # Close the Connection
    def __closeConnection(self, conn):
        try:
            conn.close()

        except exceptions.AMQPError as amqExp:
            raise CommunicationExp("Connection did not close Properly. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_SCHED_CLOSE)

    #
    # Handle incoming RPC Requests
    #
    def __on_rpc_request(self, channel, method, props, body):
        # The Request content will be passed to the callback function and
        # the result will be sent back as the response to this RPC call
        request = str(body.decode("utf-8"))
        # Response will be put in this mutable list
        response = []
        # Keep the connection open for 30 Secs
        timer = 30
        event_thr = Event()
        # Run the call_back function to make sure connection stays open until the results
        # are ready or time is up
        func_thr = Thread(target=self.__rpc_callback_func, args=(request, response, event_thr,))
        func_thr.start()

        while func_thr.is_alive():
            # The connection stays open for no more than 30 Secs
            if timer:
                event_thr.set()
            channel._connection.sleep(1.0)
            timer -= 1

        if not response:
            response.append('NONE')

        # Send back the Response to RPC Client
        channel.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=BasicProperties(correlation_id=props.correlation_id),
            body=str(response[0])
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    #
    # Start RPC Server and make it listening for RPC requests from client
    #   The 'callback' function will be defined later to receive a string input
    #   as the request and and a list to return the result and process an String
    #   output as the response
    #
    def start_RPC_server(self, callback_func: Callable[[str, list, 'Event'], None]):
        self.__rpc_callback_func = callback_func
        # sets up the consumer prefetch to only be delivered one message
        # at a time. The consumer must acknowledge this message before
        # RabbitMQ will deliver another one.
        self.__channel.basic_qos(prefetch_count=1)
        # Set basic consumer with a "callback" function
        self.__channel.basic_consume(
            queue=self.__rpc_queue,
            on_message_callback=self.__on_rpc_request,
            #auto_ack=True
        )
        # Start consuming: Listening to the channel and collecting the
        # incoming IO stats from agents
        self.__channel.start_consuming()

#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class CommunicationExp(Exception):
    def __init__(self, message, expType):
        super(CommunicationExp, self).__init__(message)
        self.message = message
        self.expType = expType

    def getMessage(self):
        return "\n [Error] _{}_: {} \n".format(self.expType.name, self.message)

    #
    # Communication Exception may have different causes
    class Type(Enum):
        AMQP_SCHED__CONFIG = 1
        AMQP_SCHED_CONN = 2
        AMQP_SCHED_CHANL = 3
        AMQP_SCHED_CLOSE = 4


def myCallback(data: str) -> str:
    return "I received [" + data + "] from you!"

if __name__ == "__main__":
    schedComm = SchedConnection(is_rpc=True)
    schedComm.start_RPC_server(myCallback)
