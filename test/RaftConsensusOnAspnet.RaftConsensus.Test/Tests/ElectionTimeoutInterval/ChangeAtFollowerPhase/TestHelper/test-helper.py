#!/usr/bin/env python3

import OutputHelper
import RequestSender
import signal
import subprocess
from datetime import datetime
from time import sleep

TOLERANCE_TIME = 2

# start the test environment
ip = "http://node:7195"
process = subprocess.Popen(["python3", "./RequestHandler.py"], stdout=subprocess.PIPE , text=True)

# core test logic in here
RequestSender.post_election_timeout_interval(ip, 2000)
sleep(2)
expect_time = datetime.now()
print(f"expect_time = {expect_time}")
sleep(TOLERANCE_TIME)

# stop test environment, extract test message and print loggings from fake node
RequestSender.stop_node(ip)
process.send_signal(signal.SIGINT)
stdout , stderr = process.communicate()
print("Output:")
outputs , test_messages = OutputHelper.extract_outputs(stdout)
for output in outputs:
    print(output)
if stderr is not None:
    print("\nError:")
    print(stderr)
print("\n")  # double new line

# get result from fake node
# TODO
