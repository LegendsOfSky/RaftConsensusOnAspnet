#!/usr/bin/env python3
import os
import output_helper
import request_handler
import request_sender
import signal
import socket
import subprocess
import sys
from datetime import datetime
from time import sleep

pipe = None


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
try:
    os.mkfifo(request_handler.PIPE_NOTIFICATION_CHANNEL)
except FileExistsError:
    pass
pipe = os.open(request_handler.PIPE_NOTIFICATION_CHANNEL, os.O_RDWR , os.O_RDONLY)
process = subprocess.Popen(["python3", "./request_handler.py"], stdout=subprocess.PIPE , text=True)

''' core test logic in here '''
election_timeout_interval = 1
request_sender.post_election_timeout_interval(IP, election_timeout_interval * 1000)
read_from_notification_pipe(pipe)
election_timeout_interval = 2
request_sender.post_election_timeout_interval(IP, election_timeout_interval * 1000)
sleep(election_timeout_interval)
expect_time = datetime.now()
actual_time = datetime.strptime(read_from_notification_pipe(pipe) , request_handler.TIME_FORMAT)

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
abs_time_diff = abs(actual_time - expect_time)
time_deviation = abs_time_diff.total_seconds()
if time_deviation <= MAX_TIME_DEVIATION:
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
