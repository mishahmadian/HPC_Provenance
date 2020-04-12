# -*- coding: utf-8 -*-
"""
    RabbitMQ Interface: Send/publish the collected logs to
    the Monitoring server.

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties,  exceptions
from .config import ServerConfig, ConfigReadExcetion
from .logger import log, Mode
from uuid import uuid4
from enum import Enum

class ServerConnection:
    def __init__(self, is_rpc=False):

        try:
            self.__config = ServerConfig()
            self.__server = self.__config.getServer()
            self.__port = self.__config.getPort()
            self.__username = self.__config.getUsername()
            self.__password = self.__config.getPassword()
            self.__Vhost = self.__config.getVhost()

        except ConfigReadExcetion as confExp:
            log(Mode.COMMUNICATION, confExp.getMessage())

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

        # Initiate RPC Client
        self.__is_rpc = is_rpc
        self.__rpc_callback_queue = None
        self.__rpc_id = None
        self.__rpc_response = None
        if is_rpc:
            self.__setup_RPC()

    # Establish a Connection to RabbitMQ server
    def __openConnection(self):
        try:
            return BlockingConnection(self.__params)

        except (exceptions.ConnectionClosed, exceptions.AMQPConnectionError,
                exceptions.AMQPError) as amqExp:
            raise CommunicationExp("Connection was not established. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_CONN)

    # Open a channel on current connection to RabbitMQ server
    def __openChannel(self, conn):
        try:
            return conn.channel()

        except(exceptions.AMQPChannelError, exceptions.AMQPError) as amqExp:
            raise CommunicationExp("couldn't open a Channel. {})".format(amqExp),
                                    CommunicationExp.Type.AMQP_CHANL)
    # Close the Connection
    def __closeConnection(self, conn):
        try:
            conn.close()

        except exceptions.AMQPError as amqExp:
            raise CommunicationExp("Connection did not close Properly. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_CLOSE)

    #
    # Listen to the queue shared between server and qgents and collect
    # I/O statistics which are generated by agents and call the "callback"
    # function to handle the received data
    #
    def Collect_io_stats(self, callback):
        # Get IO Stat Listener Params
        queue = self.__config.getIOListener_Queue()
        exchange = self.__config.getIOListener_Exch()
        # Declare a direct Exchange for this listener
        self.__channel.exchange_declare(exchange=exchange, exchange_type='direct')
        # Declare the queue
        self.__channel.queue_declare(queue=queue)
        # Bind Queue to Exchange
        self.__channel.queue_bind(exchange=exchange, queue=queue)
        # sets up the consumer prefetch to only be delivered one message
        # at a time. The consumer must acknowledge this message before
        # RabbitMQ will deliver another one.
        self.__channel.basic_qos(prefetch_count=1)
        # Set basic consumer with a "callback" function
        self.__channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        # Start consuming: Listening to the channel and collecting the
        # incoming IO stats from agents
        self.__channel.start_consuming()
        #while channel._consumer_infos:
        #    channel.connection.process_data_events(time_limit=1)
        #
        return self.__conn, self.__channel

    #
    # Set RPC Client Response
    # on_response is a function that will be called when a response is received
    #      on_response(ch, method, props, body)
    def __setup_RPC(self):
        
        # Prepare Channel & Queue to receive RPC response
        result = self.__channel.queue_declare(queue='', exclusive=True)
        self.__rpc_callback_queue = result.method.queue
        
        # Consume the channel for response
        self.__channel.basic_consume(
            queue=self.__rpc_callback_queue,
            on_message_callback=self.__on_rpc_response,
            auto_ack=True
        )

    # Handle the RPC Response that comes from RPC Server
    # the rpc_id ensures the response belongs to this request call
    def __on_rpc_response(self, ch, method, props, body):
        if self.__rpc_id == props.correlation_id:
            self.__rpc_response = str(body.decode("utf-8"))

    #
    # The main function that calls the RPC request to Server and receives the response
    #  Each 'rpc_queue" relates to one specific Remote Procedure on Server
    #
    def rpc_call(self, rpc_queue: str, input_data : str) -> str:
        # rpc_queue should be defined either by constructor or this method
        if not self.__is_rpc:
            raise CommunicationExp("The ServerConnection is not set for RPC [is_rpc=False]"
                                   , CommunicationExp.Type.AMQP_RPC)
        self.__rpc_response = None
        # Generate a uniq ID for this RPc call
        self.__rpc_id = str(uuid4())
        # Send/publish the request
        self.__channel.basic_publish(
            exchange='',
            routing_key=rpc_queue,
            properties=BasicProperties(
                reply_to=self.__rpc_callback_queue,
                correlation_id=self.__rpc_id,
            ),
            body=str(input_data)
        )
        # Wait for response to be received
        while not self.__rpc_response:
            self.__conn.process_data_events()

        # Return the result in a form of String
        return str(self.__rpc_response)


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
        AMQP_CONFIG = 1
        AMQP_CONN = 2
        AMQP_CHANL = 3
        AMQP_CLOSE = 4
        AMQP_RPC = 5
