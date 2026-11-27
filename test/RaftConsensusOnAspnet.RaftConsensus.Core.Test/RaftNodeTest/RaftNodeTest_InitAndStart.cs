using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

// ReSharper disable once InconsistentNaming
[Collection(nameof(RaftNodeTest_InitAndStart))]
[CollectionDefinition("Raft node initialization test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTest_InitAndStart : RaftNodeTest_Base
{
    private const string InitialGuidFragment = "23e18bb6";


    public RaftNodeTest_InitAndStart(ITestOutputHelper testOutputIn) : base(testOutputIn) { }


    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task ConstructorInit_ElectionTimeoutInit_StartElectionAfterNodeStartAtSpecificInterval(int electionInterval)
    {
        Task waitUnitTestTimeout = Task.Delay((electionInterval + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            TaskCompletionSource nodeBecomeElectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            InitTraces(
                    nameof(ConstructorInit_ElectionTimeoutInit_StartElectionAfterNodeStartAtSpecificInterval) , electionInterval.ToString() ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-3d3b-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = electionInterval ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 1 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            Task waitUntilExpectTime = Task.Delay(electionInterval);

            DateTime expectTime = DateTime.MinValue , actualTime = DateTime.MinValue;
            List<Task> tasks = [waitUntilExpectTime , nodeBecomeElectionTcs.Task];
            while (expectTime == DateTime.MinValue || actualTime == DateTime.MinValue)
            {
                Task completedTask = await Task.WhenAny(tasks);
                if (completedTask == waitUntilExpectTime)
                    expectTime = DateTime.Now;
                else if (completedTask == nodeBecomeElectionTcs.Task)
                    actualTime = DateTime.Now;
                tasks.Remove(completedTask);
            }

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                nodeBecomeElectionTcs.SetResult();
                node.Stop();
                return Task.CompletedTask;
            }
        }
    }

    #region Restart With/Without Existing Data
    [Fact]
    private async Task ConstructorInit_RestartWithKeepExistData_RaftRestartAsFollowerNodeWithExistData()
    {
        const string SecondGuidFragment = "29d3";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            InitTraces(
                    nameof(ConstructorInit_RestartWithKeepExistData_RaftRestartAsFollowerNodeWithExistData) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

            // run node for initial data generation
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
            Task round1 = node.StartAsync();
            AppendEntriesReply reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 2 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry()] ,
                    }
                );
            Assert.True(reply.AppendSuccess);
            await Task.Delay(200);
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(2 , node.LogEntries.Count);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = false ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Task round2 = node.StartAsync();
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(2 , node.LogEntries.Count);
            node.Stop();
            await round2;
        }
    }

    [Fact]
    private async Task ConstructorInit_RestartWithKeepExistData_RaftRestartWithVoteInfoRestored()
    {
        const string SecondGuidFragment = "b50f";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply reply;

            InitTraces(
                    nameof(ConstructorInit_RestartWithKeepExistData_RaftRestartWithVoteInfoRestored) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");
            Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");

            // run node for initial data generation
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
            Task round1 = node.StartAsync();
            reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,
                        ReceiverId = node.NodeId ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                    }
                );
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = int.MaxValue ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 3 ,
                        RemoveExistData = false ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            Task round2 = node.StartAsync();
            reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,
                        ReceiverId = node.NodeId ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                    }
                );
            Assert.True(reply.VoteGranted);
            reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = node3Id ,
                        RequesterTerm = node.CurrentTerm ,
                        ReceiverId = node.NodeId ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                    }
                );
            Assert.False(reply.VoteGranted);
            node.Stop();
            await round2;
        }
    }

    [Fact]
    private async Task ConstructorInit_RestartWithRemoveExistData_RaftRestartAsNewNode()
    {
        const int ElectionTimeoutInterval = 500;
        const int HeartBeatInterval = 500;
        const string SecondGuidFragment = "4cd9";

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + HeartBeatInterval + ExtendedPeriod) * 3 * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<bool> entriesAppended = Channel.CreateUnbounded<bool>();
            InitTraces(
                    nameof(ConstructorInit_RestartWithRemoveExistData_RaftRestartAsNewNode) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");
            Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");

            // run node for initial data generation
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = HeartBeatInterval ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.SendAppendEntriesRequestsToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round1 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = HeartBeatInterval ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.SendAppendEntriesRequestsToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round2 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round2;

            // round three to double-check if it works
            node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = HeartBeatInterval ,
                        NodeCount = 3 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.SendAppendEntriesRequestsToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round3 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round3;

            return;


            Func<Guid , Guid , int , int , Task> CreateSendVoteRequestToOtherNodesFunc(RaftNode nodeIn)
                => async (requestId ,  requesterId ,  _ ,  _) =>
                {
                    VoteRequestReply replyTemplate = new VoteRequestReply
                    {
                        RequestId = requestId ,
                        ReceiverId = requesterId ,
                        ReplierTerm = node.CurrentTerm ,

                        TermOfRequest = node.CurrentTerm ,
                        VoteGranted = true ,
                    };
                    await nodeIn.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node2Id });
                    await nodeIn.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node3Id });
                };

            Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task> CreateAppendEntriesToOtherNodesFunc(RaftNode nodeIn)
                => async (requestId ,  requesterId ,  _ , logEntries ,  _) =>
                {
                    AppendEntriesReply replyTemplate = new AppendEntriesReply
                    {
                        RequestId = requestId ,
                        ReceiverId = requesterId ,
                        ReplierTerm = node.CurrentTerm ,

                        AppendSuccess = true ,
                        MatchIndex = logEntries.Count - 1 ,
                    };
                    await nodeIn.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node2Id });
                    await nodeIn.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node3Id });
                    await entriesAppended.Writer.WriteAsync(true);
                };
        }
    }

    [Fact]
    private async Task ConstructorInit_RestartWithRemoveExistData_RaftRestartWithoutVoteInfoRestored()
    {
        const string SecondGuidFragment = "bca1";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply reply;

            InitTraces(
                    nameof(ConstructorInit_RestartWithRemoveExistData_RaftRestartWithoutVoteInfoRestored) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");
            Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");

            // run node for initial data generation
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
            Task round1 = node.StartAsync();
            reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,
                        ReceiverId = node.NodeId ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                    }
                );
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
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
            Task round2 = node.StartAsync();
            reply = node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = node3Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,
                        ReceiverId = node.NodeId ,
                        RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                        RequesterLastLogTerm = node.LogEntries[^1].Term ,
                    }
                );
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round2;
        }
    }
    #endregion
}
