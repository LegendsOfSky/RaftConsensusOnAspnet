using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.RaftNodeTest;

// ReSharper disable once InconsistentNaming
[Collection(nameof(RaftNodeTest_HandleAppendEntries))]
[CollectionDefinition($"{nameof(RaftNode.HandleAppendEntries)} test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTest_HandleAppendEntries : RaftNodeTest_Base
{
    private const string InitialGuidFragment = "6aa36f26";


    public RaftNodeTest_HandleAppendEntries(ITestOutputHelper testOutputIn) : base(testOutputIn) { }


    #region Test over `AppendEntries RPC Receiver Implementation 1`, as written in Raft specification
    [Fact]
    private async Task HandleAppendEntries_RequesterTermEqualToRequestee_ReplyAppendSuccess()
    {
        const string SecondGuidFragment = "8838";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_RequesterTermEqualToRequestee_ReplyAppendSuccess) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but rejected.");
            Assert.Equal(0 , node.CurrentTerm);

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesterTermHigherThanRequestee_ReplyAppendSuccess()
    {
        const string SecondGuidFragment = "4bd4";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_RequesterTermHigherThanRequestee_ReplyAppendSuccess) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesterTermLowerThanRequestee_ReplyAppendFail()
    {
        const string SecondGuidFragment = "62b1";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_RequesterTermLowerThanRequestee_ReplyAppendFail) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm - 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            node.Stop();
            await nodeStart;
        }
    }
    #endregion

    #region Test over `AppendEntries RPC Receiver Implementation 2`, as written in Raft specification
    [Fact]
    private async Task HandleAppendEntries_TermOfPreviousLogDoesNotMatchBetweenRequesterAndRequestee_ReplyAppendFail()
    {
        const string SecondGuidFragment = "d9b2";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_TermOfPreviousLogDoesNotMatchBetweenRequesterAndRequestee_ReplyAppendFail) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [new LogEntry(Guid.NewGuid() , node.CurrentTerm + 1 , LogEntryOperation.None , null)] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 1 ,
                        PreviousLogTerm = node.LogEntries[^1].Term - 1 ,
                        Entries = [] ,
                    }
                );
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 1 ,
                        PreviousLogTerm = node.LogEntries[^1].Term + 1 ,
                        Entries = [] ,
                    }
                );
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesteeDoesNotHavePreviousLogAssumedByNewLeader_ReplyAppendFail()
    {
        const string SecondGuidFragment = "5845";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_RequesteeDoesNotHavePreviousLogAssumedByNewLeader_ReplyAppendFail) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = node.CurrentTerm ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 1 ,
                        PreviousLogTerm = 1 ,
                        Entries = [] ,
                    }
                );
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            node.Stop();
            await nodeStart;
        }
    }
    #endregion

    #region Test over `AppendEntries RPC Receiver Implementation 3`, as written in Raft specification
    [Fact]
    private async Task HandleAppendEntries_ExistingTermConflicts_DeleteAllFutureEntriesStartAndIncludingTheConflictingLog()
    {
        const string SecondGuidFragment = "6627";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_ExistingTermConflicts_DeleteAllFutureEntriesStartAndIncludingTheConflictingLog) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                        ] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(4 , node.LogEntries.Count);
            Assert.Equal(1 , node.CurrentTerm);
            foreach (LogEntry entry in node.LogEntries.Skip(1))
                Assert.Equal(1 , entry.Term);

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 2 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new LogEntry(Guid.NewGuid() , 2 , LogEntryOperation.None , null) ,
                            new LogEntry(Guid.NewGuid() , 2 , LogEntryOperation.None , null) ,
                        ] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(3 , node.LogEntries.Count);
            foreach (LogEntry entry in node.LogEntries.Skip(1))
                Assert.Equal(2 , entry.Term);

            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 3 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries = [] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Single(node.LogEntries);

            node.Stop();
            await nodeStart;
        }
    }
    #endregion

    #region Test over `AppendEntries RPC Receiver Implementation 4`, as written in Raft specification
    [Fact]
    private async Task HandleAppendEntries_AppendEntriesWithCorrectArgs_AppendSuccess()
    {
        const string SecondGuidFragment = "828b";

        Task waitUnitTestTimeout = Task.Delay(RegularTestTimeout * OvertimeFactor);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            AppendEntriesReply reply;

            InitTraces(
                    nameof(HandleAppendEntries_AppendEntriesWithCorrectArgs_AppendSuccess) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );
            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");

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
            reply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = node2Id ,
                        RequesterTerm = 1 ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                            new LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.None , null) ,
                        ] ,
                    }
                );
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(4 , node.LogEntries.Count);
            Assert.Equal(1 , node.CurrentTerm);
            foreach (LogEntry entry in node.LogEntries.Skip(1))
                Assert.Equal(1 , entry.Term);

            node.Stop();
            await nodeStart;
        }
    }
    #endregion
}
