# -*- coding: utf-8 -*-
"""
    RabbitMQ Interface: Send/publish the collected logs to
    the Monitoring server.

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties, exceptions
from Config import AgentConfig, ConfigReadExcetion

class Producer:
    def __init__(self):
        try:
            self.config = AgentConfig()
            self.__server = self.config.getServer()
            self.__port = self.config.getPort()
            self.__username = self.config.getUsername()
            self.__password = self.config.getPassword()
            self.__Vhost = self.config.getVhost()
            self.__prodQueue = self.config.getProd_Queue()
            self.__prodExch = self.config.getProd_Exch()
            self.__interval = self.config.getSendingInterval()

        except ConfigReadExcetion as confExp:
            raise CommunicationExp(confExp.getMessage(), CommunicationExp.Type.AMQP_CONFIG)

        # Create Credentilas with username/password
        self.__credentials = PlainCredentials(self.__username, self.__password)
        # Setup the connection parameters
        self.__params = ConnectionParameters(self.__server,
                                                self.__port,
                                                self.__Vhost,
                                                self.__credentials)

    # Establishe a Connection to RabbitMQ server
    def __openConnection(self):
        try:
            self.__connection = BlockingConnection(self.__params)

        except (exceptions.ConnectionClosedByBroker, exceptions.AMQPConnectionError,
                exceptions.AMQPError) as amqExp:
            raise CommunicationExp("Connection was not established. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_CONN)

    # Open a channel in current connection to RabbitMQ server
    def __openChannel(self):
        try:
            self.__channel = self.__connection.channel()


        except(exceptions.AMQPChannelError, exceptions.AMQPError) as amqExp:
            raise CommunicationExp("couldn't open a Channel. {})".format(amqExp),
                                    CommunicationExp.Type.AMQP_CHANL)
    # Close the Connection
    def __closeConnection(self):
        try:
            self.__connection.close()

        except (exceptions.AMQPError) as amqExp:
            raise CommunicationExp("Connection did not close successfully. {}".format(amqExp),
                                    CommunicationExp.Type.AMQP_CLOSE)
    #
    # Send the message to RabbitMQ server
    #
    def send(self, msg):
        # Establish a Connection
        self.__openConnection()
        # Open a Channel
        self.__openChannel()
        # Declare a direct Exchange to jobstat queue
        self.__channel.exchange_declare(exchange=self.__prodExch, exchange_type='direct')
        # Declare the queue for jobstats
        ##self.__channel.queue_declare(queue=self.__prodQueue)
        # Bind Queue to Exchange
        ##self.__channel.queue_bind(exchange=self.__prodExch, queue=self.__prodQueue)
        # Publish the message to Server
        self.__channel.basic_publish(exchange=self.__prodExch,
                                    routing_key=self.__prodQueue,
                                    body=msg,
                                    properties=BasicProperties(
                                         delivery_mode = 2, # make message persistent
                                      ))
        # Close Connection
        self.__closeConnection()

    # Get the Interval time between each Send()
    def getInterval(self):
        return self.__interval

#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class CommunicationExp(Exception):
    def __init__(self, message, type):
        super(CommunicationExp, self).__init__(message)
        self.message = message
        self.type = type

    def getMessage(self):
        return "\n [Error] _{}_: {} \n".format(self.type, self.message)

    #
    # Communication Exception may have different causes
    # This a stupid. Sadly, python 2.7 does not support Enum :(
    class Type:
        AMQP_CONFIG = 'AMQP_CONFIG'
        AMQP_CONN = 'AMQP_CONN'
        AMQP_CHANL = 'AMQP_CHANL'
        AMQP_CLOSE = 'AMQP_CLOSE'
