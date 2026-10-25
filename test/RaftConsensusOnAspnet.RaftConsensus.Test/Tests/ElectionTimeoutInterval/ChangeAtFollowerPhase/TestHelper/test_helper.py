#!/usr/bin/env python3
import output_helper
import request_handler
import request_sender
import signal
import subprocess
import sys
from request_handler import RequestHandler
from datetime import datetime
from time import sleep


def output_failure_and_exit():
    print("Failed")
    sys.exit(-1)


MAX_TIME_DEVIATION = 0.5
TOLERANCE_FACTOR = 2

''' start the test environment '''
ip = "http://node:7195"
process = subprocess.Popen(["python3", "./request_handler.py"], stdout=subprocess.PIPE , text=True)

''' core test logic in here '''
ELECTION_TIMEOUT_INTERVAL = 2
request_sender.post_election_timeout_interval(ip, ELECTION_TIMEOUT_INTERVAL * 1000)
sleep(ELECTION_TIMEOUT_INTERVAL)
expect_time = datetime.now()
print(f"expect_time = {expect_time}")
sleep(MAX_TIME_DEVIATION * TOLERANCE_FACTOR)

''' stop test environment, extract test message and print loggings from fake node '''
request_sender.stop_node(ip)
process.send_signal(signal.SIGINT)
stdout , stderr = process.communicate()
print("\nLog output:")
outputs , test_messages = output_helper.extract_outputs(stdout)
for output in outputs:
    print(output)
if stderr is not None:
    print("\nError:")
    print(stderr)
print("\n")  # double new line

''' verify test result '''
success = len(test_messages) > 0

extract_success , actual_time , path , query_parameters , request_body = RequestHandler.extract_info_from_test_output(test_messages[0])
success &= extract_success and path == request_handler.PATH_VOTE_REQUEST         \
    and query_parameters[request_handler.QUERY_REQUESTER_TERM] == "1"            \
    and query_parameters[request_handler.QUERY_REQUESTER_LAST_LOG_TERM] == "0"   \
    and query_parameters[request_handler.QUERY_REQUESTER_LAST_LOG_INDEX] == "0"
if not success:
    output_failure_and_exit()

abs_time_diff = abs(actual_time - expect_time)
time_deviation = abs_time_diff.total_seconds()
success &= time_deviation <= MAX_TIME_DEVIATION
if success:
    print("\nSuccess")
else:
    output_failure_and_exit()
