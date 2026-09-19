# The aim of the test is to check if raft implementation always produce a new leader with two candidates competing for votes.
# Expect behaviors need to be tested:
#  1) If the vote splits equally on two candidate (for even node count, a node must be dropped), the cluster should restart another election with higher term;
#  2) A higher term candidate should always rule out lower term even if they started the election at the same time, and have the same election timeout
#     interval;
#
# Latency must be installed BEFORE the dual election starts. Toxiproxy delay only affects bytes after the toxic exists; vote requests that already went out are
# granted immediately. Downstream delay is also useless here: the follower casts its vote when the request arrives, so only --upstream delay changes who wins
# the race.

Write-Output "Sub-test 2 started."
# 8474 -> node 0 proxy, 8478 -> node 4 proxy
toxiproxy-cli --host "localhost:8476" delete "toNode2"    # node all -> 2
toxiproxy-cli --host "localhost:8475" delete "fromNode0"  # node 0 -> 1
toxiproxy-cli --host "localhost:8477" delete "fromNode0"  # node 0 -> 3
toxiproxy-cli --host "localhost:8478" delete "fromNode0"  # node 0 -> 4
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=500"
Start-Sleep -Seconds 1.2

# Freeze both candidates so toxics can be installed without racing an in-flight election.
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=100000"
curl -X 'POST' "localhost:7194/api/nodes/4/election-timeout-interval?newValue=100000"

toxiproxy-cli --host "localhost:8475" create -l 0.0.0.0:7190 -u 127.0.0.1:7195 fromNode0  # node 0 -> 1
toxiproxy-cli --host "localhost:8477" create -l 0.0.0.0:7190 -u 127.0.0.1:7195 fromNode0  # node 0 -> 3
toxiproxy-cli --host "localhost:8478" create -l 0.0.0.0:7190 -u 127.0.0.1:7195 fromNode0  # node 0 -> 4
toxiproxy-cli --host "localhost:8474" toxic add -t latency --upstream -a latency=500 -a jitter=0 fromNode4  # node 4 -> 0 delay 500ms
toxiproxy-cli --host "localhost:8475" toxic add -t latency --upstream -a latency=500 -a jitter=0 fromNode4  # node 4 -> 1 delay 500ms
toxiproxy-cli --host "localhost:8477" toxic add -t latency --upstream -a latency=500 -a jitter=0 fromNode0  # node 0 -> 3 delay 500ms
toxiproxy-cli --host "localhost:8478" toxic add -t latency --upstream -a latency=500 -a jitter=0 fromNode0  # node 0 -> 4 delay 500ms

# Dual election starts only after delay is in place.
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=1000"
curl -X 'POST' "localhost:7194/api/nodes/4/election-timeout-interval?newValue=1000"
Start-Sleep -Seconds 2.2
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=10000"
curl -X 'POST' "localhost:7194/api/nodes/4/election-timeout-interval?newValue=10000"
Start-Sleep -Seconds 1
Write-Output "Sub-test 2 finished. Check logs to evaluate whether the test is success or not."
Write-Output (
        "In this test case, look for 1) node 3 receive node 4's vote request first then node 0's vote request. But finally vote for node 0; 2) node 4 revert " +
        "to follower and vote for node 0"
    )