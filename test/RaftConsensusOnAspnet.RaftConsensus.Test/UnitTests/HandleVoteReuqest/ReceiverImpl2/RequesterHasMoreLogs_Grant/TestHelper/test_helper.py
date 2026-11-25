#!/usr/bin/env python3
import os
import output_helper
import request_handler
import request_sender
import select
import signal
import socket
import subprocess
import sys
import uuid
from request_data_types import LogEntries , Operation


pipe = None
ALT_NODE = uuid.UUID("00000000-0000-0000-0000-000000000003")


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
    select.select([pipe] , [] , [])
    data_length = int(os.read(pipe , 3))
    select.select([pipe] , [] , [])
    return os.read(pipe , data_length).decode()


IP = "http://node:7195"
MAX_TIME_DEVIATION = 0.5
TOLERANCE_FACTOR = 2

''' start the test environment '''
server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
server_socket.bind(("" , 7190))
server_socket.listen(1)
try:
    os.mkfifo(request_handler.PIPE_NOTIFICATION_CHANNEL)
except FileExistsError:
    pass
pipe = os.open(request_handler.PIPE_NOTIFICATION_CHANNEL , os.O_RDWR , os.O_RDONLY)
process = subprocess.Popen(["python3" , "./request_handler.py"] , stdout=subprocess.PIPE , text=True)

''' core test logic in here '''
append_reply1 = request_sender.patch_entries(
        IP , uuid.uuid4() , request_handler.FALLBACK_REPLIER_ID , 2 , 0 , 0 , 0 ,
        [
            LogEntries(uuid.uuid4() , "" , 0 , Operation.NO_OP) ,
            LogEntries(uuid.uuid4() , "" , 0 , Operation.NO_OP) ,
            LogEntries(uuid.uuid4() , "" , 0 , Operation.NO_OP)
        ]
    )
append_reply2 = request_sender.patch_entries(IP , uuid.uuid4() , request_handler.FALLBACK_REPLIER_ID , 2 , 3 , 0 , 3 , [])
vote_reply = request_sender.patch_vote(IP , uuid.uuid4() , ALT_NODE , 3 , 0 , 5)

''' stop test environment, extract test message and print loggings from fake node '''
request_sender.stop_node(IP)
process.send_signal(signal.SIGINT)
stdout , stderr = process.communicate()
print("\nLog output:")
outputs , _ = output_helper.extract_outputs(stdout)
for output in outputs:
    print(output)
if stderr is not None:
    print("\nError:")
    print(stderr)
print("\n")  # double new line

''' verify test result '''
if append_reply1.AppendSuccess and append_reply2.AppendSuccess and vote_reply.VoteGranted:
    print("\nSuccess")
    print("Wait for automated test platform to connect...")
    client_connection , client_address = server_socket.accept()
    print(f"Automated test platform has connected via {client_address}")
    client_connection.send("Passed".encode())
    print("Result has been sent to automated test platform.")
    client_connection.close()
    server_socket.close()
elif not (append_reply1.AppendSuccess and append_reply2.AppendSuccess):
    print(f"Expect append success, but get {append_reply1.AppendSuccess}, {append_reply2.AppendSuccess} for append entries")
    print(f"Detailed for append entries response 1: {append_reply1}")
    print(f"Detailed for append entries response 2: {append_reply2}")
    output_failure_and_exit()
else:
    print(f"Expect rejected, but get {vote_reply.VoteGranted} for granted")
    print(f"Detailed for vote response: {vote_reply}")
    output_failure_and_exit()
