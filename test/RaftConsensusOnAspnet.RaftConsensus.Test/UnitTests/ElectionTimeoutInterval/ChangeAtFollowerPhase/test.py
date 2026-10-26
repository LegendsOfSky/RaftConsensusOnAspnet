import subprocess
from time import sleep


docker_start_result = subprocess.run(["docker" , "compose" , "up" , "--build" , "-d"])
if docker_start_result.returncode != 0:
    print("Docker start failed")
    exit(-1)

# Core test logic
sleep(10)

docker_stop_result = subprocess.run(["docker" , "compose" , "rm" , "-s" , "-f"])
if docker_stop_result.returncode != 0:
    print("Docker stop failed. You may need to perform manual clean up.")
