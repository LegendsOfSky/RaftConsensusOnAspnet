#!/usr/bin/env python3

import RequestHandler
import RequestSender
import uuid
from RequestSender import LogEntries , Operation
from time import sleep


def main(host="0.0.0.0", port=7195):
    RequestHandler.start_handler(host, port)


if __name__ == "__main__":
    ip = "http://node:7195"
    RequestSender.put_entries(
        ip , uuid.uuid4() , uuid.UUID("00000000-0000-0000-0000-000000000002") , 16 , 0 , 0 , 0 ,
        [LogEntries(uuid.uuid4() , "NonValue" , 1 , Operation.NO_OP , None , None)])
    sleep(2)
    RequestSender.stop_node(ip)
