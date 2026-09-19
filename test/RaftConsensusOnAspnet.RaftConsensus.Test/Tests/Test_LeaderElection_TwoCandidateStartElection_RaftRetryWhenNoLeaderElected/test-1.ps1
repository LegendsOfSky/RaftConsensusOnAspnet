# The aim of the test is to check if raft implementation always produce a new leader with two candidates competing for votes.
# Expect behaviors need to be tested:
#  1) If the vote splits equally on two candidate (for even node count, a node must be dropped), the cluster should restart another election with higher term;
#  2) A higher term candidate should always rule out lower term even if they started the election at the same time, and have the same election timeout
#     interval;

Write-Output "Sub-test 1 started."
# 8474 -> node 0 proxy, 8478 -> node 4 proxy
toxiproxy-cli --host "localhost:8476" delete "toNode2"    # node all -> 2
toxiproxy-cli --host "localhost:8477" delete "fromNode0"  # node 0 -> 3
toxiproxy-cli --host "localhost:8478" delete "fromNode0"  # node 0 -> 4
toxiproxy-cli --host "localhost:8474" delete "fromNode4"  # node 4 -> 0
toxiproxy-cli --host "localhost:8475" delete "fromNode4"  # node 4 -> 1
curl -X 'POST' "localhost:7194/api/nodes/4/election-timeout-interval?newValue=2000"
Start-Sleep -Seconds 1
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=1000"
Start-Sleep -Seconds 1.3
# Recreate the same chain as ToxiproxyConfigs: fromNode* -> 127.0.0.1:7195 (this container's toNode) -> nodeN.
toxiproxy-cli --host "localhost:8476" create -l 0.0.0.0:7195 -u node2:7195 toNode2          # node all -> 2
toxiproxy-cli --host "localhost:8477" create -l 0.0.0.0:7190 -u 127.0.0.1:7195 fromNode0  # node 0 -> 3
toxiproxy-cli --host "localhost:8478" create -l 0.0.0.0:7190 -u 127.0.0.1:7195 fromNode0  # node 0 -> 4
toxiproxy-cli --host "localhost:8474" create -l 0.0.0.0:7194 -u 127.0.0.1:7195 fromNode4  # node 4 -> 0
toxiproxy-cli --host "localhost:8475" create -l 0.0.0.0:7194 -u 127.0.0.1:7195 fromNode4  # node 4 -> 1
Write-Output "Node 2 has joined back to the cluster."
Start-Sleep -Seconds 2
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=10000"
curl -X 'POST' "localhost:7194/api/nodes/4/election-timeout-interval?newValue=10000"
Start-Sleep -Seconds 1
Write-Output "Sub-test 1 finished. Check logs to evaluate whether the test is success or not."
Write-Output "In this test case, look for an election restart with higher term."
