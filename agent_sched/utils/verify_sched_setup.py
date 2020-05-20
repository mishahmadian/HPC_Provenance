#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Check all the lustre agent setups and look for any dysfunctional behavior.
    The following modules will be checked:
        - Communication with server

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
import socket
import sys
# Access to the 'packages' directory for all python files
sys.path.append('../packages/')
from schedComm import SchedConnection, CommunicationExp

def main():
    try:
        schedConn = SchedConnection(is_rpc=True)
        schedConn.start_RPC_server(callback)

        print("An RPC Request was recieved and a reply was sent successfully")

    except CommunicationExp as commExp:
        print(f"[Error]: {commExp}")

    except Exception as exp:
        print(f"[Error]: {exp}")

def callback(msg):
    print(f"Recieved an RPC request from [{msg}]")
    return f"[{socket.gethostname()}] recieved an RPC message from [{msg}]"

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass