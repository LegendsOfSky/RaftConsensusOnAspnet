#!/usr/bin/env python3

import RequestHandler
import RequestSender
from threading import Thread
from time import sleep


def main(host="0.0.0.0", port=7195):
    RequestHandler.start_handler(host, port)


if __name__ == "__main__":
    ip = "http://node:7195"
    receiver_thread = Thread(target=RequestHandler.start_handler , args=("0.0.0.0", 7195))
    receiver_thread.start()

    RequestSender.post_election_timeout_interval(ip , 2000)
    sleep(10)

    RequestSender.stop_node(ip)
    RequestHandler.stop_handler()
    receiver_thread.join()
