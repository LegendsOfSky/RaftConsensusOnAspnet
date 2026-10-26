#!/usr/bin/env python3
import os
import output_helper
import request_handler
import request_sender
import signal
import socket
import subprocess
import sys
from request_handler import RequestHandler
from datetime import datetime
from time import sleep


def output_failure_and_exit():
    print("Failed")
    print("Wait for automated test platform to connect...")
    client_connection , client_address = server_socket.accept()
    print(f"Automated test platform has connected via {client_address}")
    client_connection.send("Failed".encode())
    print("Result has been sent to automated test platform.")
    client_connection.close()
    server_socket.close()
    sys.exit(-1)

def read_from_notification_pipe(pipe):
    data_length = int(os.read(pipe , 3))
    return os.read(pipe, data_length).decode()


IP = "http://node:7195"
MAX_TIME_DEVIATION = 0.5
TOLERANCE_FACTOR = 2

''' start the test environment '''
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("", 7190))
server_socket.listen(1)
process = subprocess.Popen(["python3", "./request_handler.py"], stdout=subprocess.PIPE , text=True)

''' core test logic in here '''
ELECTION_TIMEOUT_INTERVAL = 2
request_sender.post_election_timeout_interval(IP, ELECTION_TIMEOUT_INTERVAL * 1000)
sleep(ELECTION_TIMEOUT_INTERVAL)
expect_time = datetime.now()
print(f"expect_time = {expect_time}")
sleep(MAX_TIME_DEVIATION * TOLERANCE_FACTOR)

''' stop test environment, extract test message and print loggings from fake node '''
request_sender.stop_node(IP)
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
    print("Wait for automated test platform to connect...")
    client_connection , client_address = server_socket.accept()
    print(f"Automated test platform has connected via {client_address}")
    client_connection.send("Passed".encode())
    print("Result has been sent to automated test platform.")
    client_connection.close()
    server_socket.close()
else:
    output_failure_and_exit()
