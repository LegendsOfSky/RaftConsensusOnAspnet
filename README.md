# Raft Consensus On ASP.NET
A Raft consensus implementation on the ASP.NET stack.

Use it as a **standalone replica unit** that replicates data across machines, or embed the **core library** in another C# program for native consensus.

> [!WARNING]
> This project exists to practice and demonstrate the Raft algorithm. It is **not production-ready**. Treat it as a learning / prototype codebase: review persistence, networking, membership changes, and failure handling before any real deployment.

Licensed under the [MIT License](LICENSE).

## Contents
- [What it does](#what-it-does)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Usage](#usage)
- [HTTP API for standalone unit](#http-api-for-standalone-unit)
- [Core library API](#core-library-api)
- [Configuration on Raft Unit](#configuration-on-raft-unit)
- [Development and tests](#development-and-tests)
- [Credits](#credits)


## What it does
### Standalone unit
- Provide Linearizable consistent read across replicas;
- Grantuee every success propose is persisted in the cluster even if some nodes crash (network drop, power loss, process kill);

### Embeddable core (template)
- Easy to embed to other C# source code or/and to use other interconnect protocal;
- Easy to add custom key-value log entry types;
- Easy to setup logging for standard info and debug info via user provided `TraceListener`s;
- Easy to create nested Raft clusters (small amount of coding is required so append-entries always go out as proposes to the leader of the nested cluster);


## Requirements
**Build**
- [.NET 10 SDK](https://dotnet.microsoft.com/download)

**Full test suite**
- Everything listed for building project
- Docker running on WSL
- [toxiproxy-cli](https://github.com/shopify/toxiproxy)
- Powershell 7


## Quick start
### Build
```bash
git clone https://github.com/LegendsOfSky/RaftConsensusOnAspnet.git
cd RaftConsensusOnAspnet
dotnet build -c Release
```


### Run a single node
```bash
cp -r src/RaftConsensusOnAspnet.RaftConsensus/bin/Release/net10.0/* /desire_app_location
cd /desire_app_location
.\RaftConsensusOnAspnet.RaftConsensus --timeout-interval <interval in millisecond> --heart-beat-interval <interval in millisecond> --id <node id>
```
**Important Notes:**
- Changing config file or changing `--id` arguments may cause Raft node to not starting until `--id` exist in config file;
- Printing stack trace error is normal when any peers offline. It is the default behavior when HTTP connection cannot be created.

**Starting a long waiting node for testing**
```bash
.\RaftConsensusOnAspnet.RaftConsensus --timeout-interval 100000 --heart-beat-interval 20000 --id "00000000-0000-0000-0000-000000000001"
```

**Sample request**
```bash
curl -X 'POST' "localhost:7195/api/node/election-timeout-interval?newValue=2000"
```


### Run quick test enviroment
```bash
cd test/RaftConsensusOnAspnet.RaftConsensus.Test/Tests/Test_RunInDockerCompose_Runs
docker compose up --build

# sample curl
curl -X 'POST' "localhost:7195/api/node/election-timeout-interval?newValue=2000"
curl -X 'POST' "localhost:7196/api/node/election-timeout-interval?newValue=2000"
curl -X 'POST' "localhost:7197/api/node/election-timeout-interval?newValue=2000"
curl -X 'POST' "localhost:7198/api/node/election-timeout-interval?newValue=2000"
curl -X 'POST' "localhost:7199/api/node/election-timeout-interval?newValue=2000"
```


## Usage
### Standalone unit
**Typical invocation**
```bash
.\RaftConsensusOnAspnet.RaftConsensus.exe --help  # optional; for providing flag/arg helps
.\RaftConsensusOnAspnet.RaftConsensus --id "00000000-0000-0000-0000-000000000001"
```

Restrict Raft-private endpoints with a reverse proxy. Example Nginx:
```nginxconf
server {
    listen 7195;
    listen [::]:7195;
    server_name _;

    location ~ /api/nodes/(\d+)/(entries|vote)$ {
        return 400 "This request is only available for inter-communication between raft nodes.";
    }
    location ~ ^/api/nodes/(\d+)/(.*)$ {
        set $backend "http://node$1:7195";
        proxy_pass $backend/api/node/$2$is_args$args;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```


## HTTP API for standalone unit
`/api/node/entries` and `/api/node/vote` are inter-node protocol endpoints. They are not meant for application users or embedding developers. Their request bodies are omitted here on purpose; restrict them at the proxy.

| Path | Method | Scope | Parameters | Purpose |
|------|--------|-------|------------|---------|
| `/api/node` | DELETE | Private | — | Stop this Raft node |
| `/api/node/entries` | PUT | In-cluster only | hidden | Receive log entries from the leader |
| `/api/node/heart-beat-interval` | POST | Private | `newValue` (int) | Interval between append-entries this node sends while it is leader |
| `/api/node/election-timeout-interval` | POST | Private | `newValue` (int) | How long a follower waits with no contact before becoming a candidate, and how long a candidate waits before starting a new election |
| `/api/node/vote` | PATCH | In-cluster only | hidden | Receive a vote request from a candidate |


## Core library API
`RaftConsensusOnAspnet.RaftConsensus.Core` is the embeddable engine.
| Member | Kind | Role |
|--------|------|------|
| `VoteRequestReplyChannel` | Channel | Submit vote replies into the core |
| `AppendEntriesReplyChannel` | Channel | Submit append-entries replies into the core |
| `SendVoteRequestToOtherNodes` | Delegate | Assign functions that sends a vote request to a peer |
| `AppendEntriesToOtherNodes` | Delegate | Assign functions that appends entries to a peer |
| `Start()` | Method | Start the core |
| `Stop()` | Method | Stop the core |
| `GetValue()` | Method | Read the committed value |
| `ProposeAsync()` | Method | Propose a new value (return when committed) |
| `SetElectionTimeoutInterval()` | Method | Follower wait before becoming candidate; candidate wait before a new election |
| `SetHeartBeatInterval()` | Method | Wait between heartbeats while this node is leader |
| `HandleVoteRequest()` | Method | Forward a vote request into the core. Send the reply on the requester’s `VoteRequestReplyChannel` |
| `HandleAppendEntries()` | Method | Forward an append-entries request into the core. Send the reply on the requester’s `AppendEntriesReplyChannel` |


## Configuration on Raft Unit
### Flags
```bash
.\RaftConsensusOnAspnet.RaftConsensus.exe --help
```

### Cluster Config
The cluster config must be presence for the Raft unit to run. The unit reads the config on startup. By default it reads `configs/cluster.settings.json` and fallback to `configs/sample.cluster.settings.json` when missing prior. Inside the config file stores all Raft nodes within the cluster.

Definition of attribute for each node in the cluster config:
| Attribute | Definition | Remark |
|-----------|------------|--------|
| Name | Name of the node | The name does not take any effect. |
| NodeId | 128 bits GUID associate to the node | <ul><li>Every node must have their `NodeId` whitelist here;</li><li>Check [C# Guid.Parse Method](https://learn.microsoft.com/en-us/dotnet/api/system.guid.parse?view=net-10.0#system-guid-parse(system-string)) for what string format can be parsed;</li><li>Do not use `00000000-0000-0000-0000-000000000000` in here;</li></ul> |
| Ip | Ip address or root url of the node | Include `http://` or `https://` at the start. |

Example:
```json
{
  "Nodes": [
    {
      "Name": "Node 0",
      "NodeId": "00000000-0000-0000-0000-000000000001",
      "Ip": "http://node0:7195"
    },
    {
      "Name": "Node 1",
      "NodeId": "00000000-0000-0000-0000-000000000002",
      "Ip": "http://node1:7195"
    },
    {
      "Name": "Node 2",
      "NodeId": "00000000-0000-0000-0000-000000000003",
      "Ip": "http://node2:7195"
    },
    {
      "Name": "Node 3",
      "NodeId": "00000000-0000-0000-0000-000000000004",
      "Ip": "http://node3:7195"
    },
    {
      "Name": "Node 4",
      "NodeId": "00000000-0000-0000-0000-000000000005",
      "Ip": "http://node4:7195"
    }
  ]
}
```


## Development and tests
### Test on RaftConsensusOnAspnet.RaftConsensus
```powershell
git clone https://github.com/LegendsOfSky/RaftConsensusOnAspnet.git
cd .\RaftConsensusOnAspnet\test\RaftConsensusOnAspnet.RaftConsensus.Test

cd .\target_test
.\test.ps1  # invoke test script(s)
```

### Test on RaftConsensusOnAspnet.RaftConsensus.Core
```powershell
git clone https://github.com/LegendsOfSky/RaftConsensusOnAspnet.git
cd RaftConsensusOnAspnet\test\RaftConsensusOnAspnet.RaftConsensus.Core.Test
dotnet test
```
The current test is ported from CUHK's poorly designed unit test, therefore it will fail easily over Linux machine because of CPU scheduling issues. Hence the test should be run on Microsoft Windows enviroment to provide smoother timing delays for those tests. Tests will be changed on the future to have a much clear objective and less subjective to CPU scheduling.


## Credits
- [CUHK Raft consensus assignment (Fall 2025-2026 Term 2)](https://github.com/LegendsOfSky/cuhk-raft-LegendsOfSky)
- Paper `In Search of an Understandable Consensus Algorithm (Extended Version)` by Diego Ongaro and John Ousterhout
- [RichardLitt/standard-readme](https://github.com/RichardLitt/standard-readme)
