toxiproxy-cli --host "localhost:8475" delete "toNode1"
toxiproxy-cli --host "localhost:8476" delete "toNode2"
toxiproxy-cli --host "localhost:8477" delete "toNode3"
toxiproxy-cli --host "localhost:8478" delete "toNode4"

# test node 1 become leader when 3 votes received
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=1000"
Start-Sleep -Seconds 1.5
toxiproxy-cli --host "localhost:8475" create -l 0.0.0.0:7195 -u node1:7195 toNode1
Start-Sleep -Seconds 1
toxiproxy-cli --host "localhost:8476" create -l 0.0.0.0:7195 -u node2:7195 toNode2

Start-Sleep -Seconds 3

# reset test enviroment
toxiproxy-cli --host "localhost:8475" delete "toNode1"
toxiproxy-cli --host "localhost:8476" delete "toNode2"
curl -X 'POST' "localhost:7194/api/nodes/0/election-timeout-interval?newValue=10000"

Start-Sleep -Seconds 3

# test node 2 become leader when 3 votes received
curl -X 'POST' "localhost:7194/api/nodes/1/election-timeout-interval?newValue=1000"
Start-Sleep -Seconds 1.5
toxiproxy-cli --host "localhost:8477" create -l 0.0.0.0:7195 -u node3:7195 toNode3
Start-Sleep -Seconds 1
toxiproxy-cli --host "localhost:8478" create -l 0.0.0.0:7195 -u node4:7195 toNode4
