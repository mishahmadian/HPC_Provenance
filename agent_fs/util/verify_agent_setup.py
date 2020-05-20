#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Check all the lustre agent setups and look for any dysfunctional behavior.
    The following modules will be checked:
        - Communication with server

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, BasicProperties, exceptions
import socket
import sys
# Access to the 'packages' directory for all python files
sys.path.append('../packages/')
from Config import AgentConfig, ConfigReadExcetion

def main():
    try:
        config = AgentConfig()
        server = config.getServer()
        port = config.getPort()
        username = config.getUsername()
        password = config.getPassword()
        Vhost = config.getVhost()

        credentials = PlainCredentials(username, password)
        params = ConnectionParameters(host=server, port=port, virtual_host=Vhost, credentials=credentials)

        conn = BlockingConnection(params)
        channel = conn.channel()

        channel.exchange_declare(exchange="test", exchange_type='direct')
        channel.basic_publish(exchange="test",
                                    routing_key="test",
                                    body=socket.gethostname(),
                                    properties=BasicProperties(
                                         delivery_mode = 2, # make message persistent
                                    ))
        conn.close()
        print("The hostname was sent successfully to server")

    except ConfigReadExcetion as confExp:
        print("[Config File Error]: " + confExp.getMessage())
        sys.exit(1)

    except (exceptions.ConnectionClosed, exceptions.AMQPConnectionError):
        print("[Connection Error]")

    except exceptions.AMQPChannelError:
        print("[Channel Error]")

    except exceptions.AMQPError:
        print("[AMQP Error]")

    except Exception as exp:
        print("[Error]: " + str(exp))

if __name__ == '__main__':
    main()