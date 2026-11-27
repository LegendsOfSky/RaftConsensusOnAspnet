using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.RaftNodeTest;

// ReSharper disable once InconsistentNaming
[Collection(nameof(RaftNodeTest_HandleVoteRequest))]
[CollectionDefinition($"{nameof(RaftNode.HandleVoteRequest)} test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTest_HandleVoteRequest : RaftNodeTest_Base
{
    private const string InitialGuidFragment = "bc516d81";


    public RaftNodeTest_HandleVoteRequest(ITestOutputHelper testOutputIn) : base(testOutputIn) { }


    #region Test over `RequestVote RPC Receiver Implementation 1`, as written in Raft specification
    [Fact]
    private async Task HandleVoteRequest_RequesterTermEqualRequesteeTerm_VoteGranted()
    {
        const string SecondGuidFragment = "9613";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            InitTraces(
                    nameof(HandleVoteRequest_RequesterTermEqualRequesteeTerm_VoteGranted) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry()] ,
                    }
                );
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(reply.VoteGranted , "Expect vote granted, but rejected");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermHigherThanRequesteeTerm_VoteGranted()
    {
        const string SecondGuidFragment = "6f32";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            InitTraces(
                    nameof(HandleVoteRequest_RequesterTermHigherThanRequesteeTerm_VoteGranted) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry()] ,
                    }
                );
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(reply.VoteGranted , "Expect vote granted, but rejected");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermLowerThanRequesteeTerm_VoteRejected()
    {
        const string SecondGuidFragment = "88b9";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            InitTraces(
                    nameof(HandleVoteRequest_RequesterTermLowerThanRequesteeTerm_VoteRejected) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry()] ,
                    }
                );
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.False(reply.VoteGranted , "Expect vote rejected, but granted");

            node.Stop();
            await nodeStart;
        }
    }
    #endregion

    #region Test over `RequestVote RPC Receiver Implementation 2`, as written in Raft specification
    [Fact]
    private async Task HandleVoteRequest_RequesterHasLessLogsCompareToRequestee_VoteRejected()
    {
        const string SecondGuidFragment = "ffd5";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply appendEntriesReply;

            InitTraces(
                    nameof(HandleVoteRequest_RequesterHasLessLogsCompareToRequestee_VoteRejected) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = node.LogEntries.Count     - 1 ,
                        PreviousLogIndex = node.LogEntries.Count - 1 ,
                        PreviousLogTerm = node.LogEntries[^1].Term ,
                        Entries = [] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 2 ,  // requester last log index < requestee last log index
                    }
                );
            Assert.False(voteResponse.VoteGranted , "Expect vote rejected, but granted.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterHasMoreLogsCompareToRequestee_VoteGranted()
    {
        const string SecondGuidFragment = "00b3";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply appendEntriesReply;

            InitTraces(
                    nameof(HandleVoteRequest_RequesterHasMoreLogsCompareToRequestee_VoteGranted) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = node.LogEntries.Count     - 1 ,
                        PreviousLogIndex = node.LogEntries.Count - 1 ,
                        PreviousLogTerm = node.LogEntries[^1].Term ,
                        Entries = [] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count ,  // requester last log index > requestee last log index
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterHasSameLogAsRequestee_VoteGranted()
    {
        const string SecondGuidFragment = "93e5";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply appendEntriesReply;

            InitTraces(
                    nameof(HandleVoteRequest_RequesterHasSameLogAsRequestee_VoteGranted) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = node.LogEntries.Count     - 1 ,
                        PreviousLogIndex = node.LogEntries.Count - 1 ,
                        PreviousLogTerm = node.LogEntries[^1].Term ,
                        Entries = [] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,  // requester last log index == requestee last log index
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForAnotherNode_VoteRejected()
    {
        const string SecondGuidFragment = "c563";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply voteResponse;

            InitTraces(
                    nameof(HandleVoteRequest_RequesteeHasVoteForAnotherNode_VoteRejected) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");
            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node3Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.False(voteResponse.VoteGranted , "Expect vote reject (because requestee has voted for another node), but granted.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForCandidate_VoteGrantedAgain()
    {
        const string SecondGuidFragment = "6af1";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply voteResponse;

            InitTraces(
                    nameof(HandleVoteRequest_RequesteeHasVoteForCandidate_VoteGrantedAgain) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted (because requestee has previously voted for requester), but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForAnotherNodeAtLowerTerm_VoteGranted()
    {
        const string SecondGuidFragment = "ed76";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply voteResponse;

            InitTraces(
                    nameof(HandleVoteRequest_RequesteeHasVoteForAnotherNodeAtLowerTerm_VoteGranted) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");
            Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            voteResponse = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node3Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                    }
                );
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }
    #endregion
}
