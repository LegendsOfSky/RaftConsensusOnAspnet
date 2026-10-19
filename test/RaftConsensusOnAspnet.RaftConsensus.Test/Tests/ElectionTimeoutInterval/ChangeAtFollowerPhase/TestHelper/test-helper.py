#!/usr/bin/env python3

import RequestSender
import signal
import subprocess
from time import sleep

# start the test environment
ip = "http://node:7195"
process = subprocess.Popen(["python3", "./RequestHandler.py"], stdout=subprocess.PIPE , text=True)

# core test logic in here
RequestSender.post_election_timeout_interval(ip, 2000)
sleep(10)

# clean up test environment
RequestSender.stop_node(ip)
process.send_signal(signal.SIGINT)

# get result from fake node
output , error = process.communicate()
print(f"output = {output}")
