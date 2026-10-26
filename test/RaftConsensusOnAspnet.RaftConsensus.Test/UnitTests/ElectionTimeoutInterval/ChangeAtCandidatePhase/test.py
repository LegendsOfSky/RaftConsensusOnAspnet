import socket
import subprocess
from time import sleep


docker_start_result = subprocess.run(["docker" , "compose" , "up" , "--build" , "-d"])
if docker_start_result.returncode != 0:
    print("Docker start failed")
    exit(-1)

try:
    ''' wait for pip install before start connecting to test environment '''
    sleep(15)
    print("Try to establish connect to test environment...")
    client_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
    client_socket.settimeout(30)
    client_socket.connect(("localhost" , 7190))
    print("Test environment connected.")
    test_result = client_socket.recv(1024)
    if test_result.decode() == "Passed":
        print("Success")
    elif test_result.decode() == "Failed":
        print("Failure")
    else:
        print(f"Invalid result from test environment. Considering it to be failure. (Get {len(test_result)} bytes: {test_result!r})")
    client_socket.close()
finally:
    ''' clean up the test environment '''
    docker_stop_result = subprocess.run(["docker" , "compose" , "rm" , "-s" , "-f"])
    if docker_stop_result.returncode != 0:
        print("Docker stop failed. You may need to perform manual clean up.")
