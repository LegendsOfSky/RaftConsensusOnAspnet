using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers;
using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models;
using Xunit.Abstractions;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

public class RaftNodeTests
{
    private const int MaxTimeDeviation = 500;
    private const string OverTimeMessage = "Test overtime";

    private static readonly Guid s_node1Guid = Guid.Parse("00000000-0000-0000-0000-000000000001");
    private static readonly Guid s_node2Guid = Guid.Parse("00000000-0000-0000-0000-000000000002");
    private static readonly Guid s_node3Guid = Guid.Parse("00000000-0000-0000-0000-000000000003");
    private static readonly Guid s_node4Guid = Guid.Parse("00000000-0000-0000-0000-000000000004");
    private static readonly Guid s_node5Guid = Guid.Parse("00000000-0000-0000-0000-000000000005");

    private readonly ITestOutputHelper testOutput;


    public RaftNodeTests(ITestOutputHelper testOutputIn) => testOutput = testOutputIn;


    #region Election Timeout Tests
    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task ConstructorInit_ElectionTimeoutInit_StartElectionAfterNodeStartAtSpecificInterval(int electionInterval)
    {
        Task waitUnitTestTimeout = Task.Delay(electionInterval * 2);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , electionInterval , int.MaxValue , 1 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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

    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetElectionTimeoutInterval_ElectionTimeoutChangedAtFollowerPhase_UseNewTimerToWaitForHeartBeatSignal(int electionInterval)
    {
        const int DelayBeforeElectionTimeoutChanged = 200;

        Task waitUnitTestTimeout = Task.Delay((DelayBeforeElectionTimeoutChanged + electionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            TaskCompletionSource nodeBecomeElectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            InitTraces(
                    nameof(SetElectionTimeoutInterval_ElectionTimeoutChangedAtFollowerPhase_UseNewTimerToWaitForHeartBeatSignal) , electionInterval.ToString() ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue  , int.MaxValue , 1 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await Task.Delay(DelayBeforeElectionTimeoutChanged);
            node.SetElectionTimeoutInterval(electionInterval);
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

    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetElectionTimeoutInterval_ElectionTimeoutChangedAtCandidatePhase_UseNewTimerToWaitForResponseBeforeTimeout(int electionInterval)
    {
        const int InitialElectionInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<DateTime> voteRequestTimeStamp = Channel.CreateUnbounded<DateTime>();
            InitTraces(
                    nameof(SetElectionTimeoutInterval_ElectionTimeoutChangedAtCandidatePhase_UseNewTimerToWaitForResponseBeforeTimeout) ,
                    electionInterval.ToString() , out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval  , int.MaxValue , 1 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await voteRequestTimeStamp.Reader.ReadAsync();
            await Task.Delay(electionInterval);
            DateTime expectTime = DateTime.Now;
            DateTime actualTime = await voteRequestTimeStamp.Reader.ReadAsync();

            node.Stop();

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                ValueTask writeTimeStamp = voteRequestTimeStamp.Writer.WriteAsync(DateTime.Now);
                node.SetElectionTimeoutInterval(electionInterval);
                await writeTimeStamp;
            }
        }
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetElectionTimeoutInterval_ElectionTimeoutChangedAtLeaderPhase_DoNothing(int electionInterval)
    {
        const int InitialElectionInterval = 500;
        const int HeartBeatInterval = 1000;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval + HeartBeatInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            InitTraces(
                    nameof(SetElectionTimeoutInterval_ElectionTimeoutChangedAtLeaderPhase_DoNothing) , electionInterval.ToString() ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval , HeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await timestamp.Reader.ReadAsync();
            await Task.Delay(HeartBeatInterval);
            DateTime expectTime = DateTime.Now;
            DateTime actualTime = await timestamp.Reader.ReadAsync();

            node.Stop();

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                await timestamp.Writer.WriteAsync(DateTime.Now);
                node.SetElectionTimeoutInterval(electionInterval);
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }
    #endregion

    #region Heart Beat Interval Test
    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetHeartBeatInterval_HeartBeatIntervalChangedAtFollowerPhase_DoNothing(int heartBeatInterval)
    {
        const int ElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionInterval + ElectionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            InitTraces(
                    nameof(SetHeartBeatInterval_HeartBeatIntervalChangedAtFollowerPhase_DoNothing) , heartBeatInterval.ToString() ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            Task waitUntilElectionStart = Task.Delay(ElectionInterval);
            await Task.Delay(250);
            node.SetHeartBeatInterval(heartBeatInterval);
            await waitUntilElectionStart;
            DateTime expectTime = DateTime.Now;
            DateTime actualTime = await timestamp.Reader.ReadAsync();

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                await timestamp.Writer.WriteAsync(DateTime.Now);
                node.SetHeartBeatInterval(heartBeatInterval);
                node.Stop();
            }
        }
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetHeartBeatInterval_HeartBeatIntervalChangedAtCandidatePhase_DoNothing(int heartBeatInterval)
    {
        const int ElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionInterval + ElectionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            InitTraces(
                    nameof(SetHeartBeatInterval_HeartBeatIntervalChangedAtCandidatePhase_DoNothing) , heartBeatInterval.ToString() ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await timestamp.Reader.ReadAsync();
            await Task.Delay(ElectionInterval);
            DateTime expectTime = DateTime.Now;
            DateTime actualTime = await timestamp.Reader.ReadAsync();

            node.Stop();

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                await timestamp.Writer.WriteAsync(DateTime.Now);
                node.SetHeartBeatInterval(heartBeatInterval);
            }
        }
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetHeartBeatInterval_HeartBeatIntervalChangedAtLeaderPhase_RestartHeartBeatTimerAndSendHeartBeatAfterNewTimerEnd(int heartBeatInterval)
    {
        const int InitialElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + InitialElectionInterval + heartBeatInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            InitTraces(
                    nameof(SetHeartBeatInterval_HeartBeatIntervalChangedAtLeaderPhase_RestartHeartBeatTimerAndSendHeartBeatAfterNewTimerEnd) ,
                    heartBeatInterval.ToString() , out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await timestamp.Reader.ReadAsync();
            await Task.Delay(heartBeatInterval);
            DateTime expectTime = DateTime.Now;
            DateTime actualTime = await timestamp.Reader.ReadAsync();

            node.Stop();

            Assert.InRange((actualTime - expectTime).Milliseconds , -MaxTimeDeviation , MaxTimeDeviation);
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                await timestamp.Writer.WriteAsync(DateTime.Now);
                node.SetHeartBeatInterval(heartBeatInterval);
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }
    #endregion

    #region HandleVoteRequest() Test
    #region Test over `RequestVote RPC Receiver Implementation 1`, as written in Raft specification
    [Fact]
    private async Task HandleVoteRequest_RequesterTermEqualRequesteeTerm_VoteGranted()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry()] ,
            });
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(reply.VoteGranted , "Expect vote granted, but rejected");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermHigherThanRequesteeTerm_VoteGranted()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry()] ,
            });
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(reply.VoteGranted , "Expect vote granted, but rejected");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermLowerThanRequesteeTerm_VoteRejected()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Advance node into term 2
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry()] ,
            });
            Assert.Equal(2 , node.CurrentTerm);

            // Core test logic
            VoteRequestReply reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
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
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = node.LogEntries.Count     - 1 ,
                PreviousLogIndex = node.LogEntries.Count - 1 ,
                PreviousLogTerm = node.LogEntries[^1].Term ,
                Entries = [] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 2 ,  // requester last log index < requestee last log index
            });
            Assert.False(voteResponse.VoteGranted , "Expect vote rejected, but granted.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterHasMoreLogsCompareToRequestee_VoteGranted()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = node.LogEntries.Count     - 1 ,
                PreviousLogIndex = node.LogEntries.Count - 1 ,
                PreviousLogTerm = node.LogEntries[^1].Term ,
                Entries = [] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count ,  // requester last log index > requestee last log index
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterHasSameLogAsRequestee_VoteGranted()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task nodeStart = node.StartAsync();
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry() , new LogEntry() , new LogEntry()] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);
            appendEntriesReply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = node.LogEntries.Count     - 1 ,
                PreviousLogIndex = node.LogEntries.Count - 1 ,
                PreviousLogTerm = node.LogEntries[^1].Term ,
                Entries = [] ,
            });
            Assert.True(appendEntriesReply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(4 , node.LogEntries.Count);

            // Core test logic
            VoteRequestReply voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,  // requester last log index == requestee last log index
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForAnotherNode_VoteRejected()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node3Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.False(voteResponse.VoteGranted , "Expect vote reject (because requestee has voted for another node), but granted.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForCandidate_VoteGrantedAgain()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted (because requestee has previously voted for requester), but rejected.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesteeHasVoteForAnotherNodeAtLowerTerm_VoteGranted()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // Add log entries to the node
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task nodeStart = node.StartAsync();

            // Core test logic
            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            voteResponse = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node3Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
            });
            Assert.True(voteResponse.VoteGranted , "Expect vote granted, but rejected.");

            node.Stop();
            await nodeStart;
        }
    }
    #endregion
    #endregion

    #region HandleAppendEntries() Test
    #region Test over `AppendEntries RPC Receiver Implementation 1`, as written in Raft specification
    [Fact]
    private async Task HandleAppendEntries_RequesterTermEqualToRequestee_ReplyAppendSuccess()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but rejected.");
            Assert.Equal(0 , node.CurrentTerm);

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesterTermHigherThanRequestee_ReplyAppendSuccess()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesterTermLowerThanRequestee_ReplyAppendFail()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm - 1 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
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
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry(Guid.NewGuid() , node.CurrentTerm + 1 , LogEntryOperation.None , null)] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 1 ,
                PreviousLogTerm = node.LogEntries[^1].Term - 1 ,
                Entries = [] ,
            });
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 1 ,
                PreviousLogTerm = node.LogEntries[^1].Term + 1 ,
                Entries = [] ,
            });
            Assert.False(reply.AppendSuccess , "Expect append entries failed, but succeed.");

            node.Stop();
            await nodeStart;
        }
    }

    [Fact]
    private async Task HandleAppendEntries_RequesteeDoesNotHavePreviousLogAssumedByNewLeader_ReplyAppendFail()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(1 , node.CurrentTerm);

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 1 ,
                PreviousLogTerm = 1 ,
                Entries = [] ,
            });
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
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
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
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(4 , node.LogEntries.Count);
            Assert.Equal(1 , node.CurrentTerm);
            foreach (LogEntry entry in node.LogEntries.Skip(1))
                Assert.Equal(1 , entry.Term);

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries =
                [
                    new LogEntry(Guid.NewGuid() , 2 , LogEntryOperation.None , null) ,
                    new LogEntry(Guid.NewGuid() , 2 , LogEntryOperation.None , null) ,
                ] ,
            });
            Assert.True(reply.AppendSuccess , "Expect append entries success, but failed.");
            Assert.Equal(3 , node.LogEntries.Count);
            foreach (LogEntry entry in node.LogEntries.Skip(1))
                Assert.Equal(2 , entry.Term);

            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 3 ,

                LeaderCommit = 0 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [] ,
            });
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
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);

            Task nodeStart = node.StartAsync();
            reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
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
            });
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
    #endregion

    #region Test on raft rules for each Raft role
    #region Common part for all raft roles
    [Fact]
    private async Task HandleAppendEntries_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower()
    {
        const int ElectionTimeoutInterval = 500;
        const int MaxTestTimeForCandidateNodeTest = (ElectionTimeoutInterval * 2) * 2;
        const int MaxTestTimeForLeaderNodeTest = (ElectionTimeoutInterval * 3) * 2;

        Task test , completedTask;

        Task waitUnitTestTimeout = Task.Delay(MaxTestTimeForCandidateNodeTest + MaxTestTimeForLeaderNodeTest);

        testOutput.WriteLine("Test over candidate node:");
        test = PerformTestOnCandidateNode();
        completedTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completedTask , OverTimeMessage);
        await test;
        testOutput.WriteLine("\n\n\n\n");
        testOutput.WriteLine("Test over leader node:");
        test = PerformTestOnLeaderNode();
        completedTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completedTask , OverTimeMessage);
        await test;

        return;


        async Task PerformTestOnCandidateNode()
        {
            InitTraces(
                    $"{nameof(HandleAppendEntries_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower)}_OnCandidateNode" , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            TaskCompletionSource nodeBecomeCandidateTcs = new  TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                LeaderCommit = 0 ,
                Entries = [] ,
            });
            await Task.Delay(100);  // Important tolerant windows. Because the append entries request has a higher term and the term update to be commited to
            //     the database. Therefore, it may possess a delay for the role update depending on implementation. This delay is to
            //     allow database to commit all necessary values before perform test check.
            Assert.Equal(NodeRole.Follower , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                nodeBecomeCandidateTcs.SetResult();
                node.SetElectionTimeoutInterval(int.MaxValue);
                return Task.CompletedTask;
            }
        }

        async Task PerformTestOnLeaderNode()
        {
            InitTraces(
                    $"{nameof(HandleAppendEntries_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower)}_OnLeaderNode" , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            TaskCompletionSource nodeBecomeLeaderTcs = new  TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeLeaderTcs.Task;
            Assert.Equal(NodeRole.Leader , node.Role);
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                LeaderCommit = 0 ,
                Entries = [] ,
            });
            await Task.Delay(200);  // Important tolerant windows. Because the append entries request has a higher term and the term update to be commited to
            //     the database. Therefore, it may possess a delay for the role update depending on implementation. This delay is to
            //     allow database to commit all necessary values before perform test check.
            Assert.Equal(NodeRole.Follower , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                nodeBecomeLeaderTcs.SetResult();
                return Task.CompletedTask;
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower()
    {
        const int ElectionTimeoutInterval = 500;
        const int MaxTestTimeForCandidateNodeTest = (ElectionTimeoutInterval * 2) * 2;
        const int MaxTestTimeForLeaderNodeTest = (ElectionTimeoutInterval * 3) * 2;

        Task test , completedTask;

        Task waitUnitTestTimeout = Task.Delay(MaxTestTimeForCandidateNodeTest + MaxTestTimeForLeaderNodeTest);

        testOutput.WriteLine("Test over candidate node:");
        test = PerformTestOnCandidateNode();
        completedTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completedTask , OverTimeMessage);
        await test;
        testOutput.WriteLine("\n\n\n\n");
        testOutput.WriteLine("Test over leader node:");
        test = PerformTestOnLeaderNode();
        completedTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completedTask , OverTimeMessage);
        await test;

        return;


        async Task PerformTestOnCandidateNode()
        {
            InitTraces(
                    $"{nameof(HandleVoteRequest_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower)}_OnCandidateNode" , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            TaskCompletionSource nodeBecomeCandidateTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                RequesterLastLogIndex = 0 ,
                RequesterLastLogTerm = 0 ,
            });
            await Task.Delay(100);  // Important tolerant windows. Because the append entries request has a higher term and the term update to be commited to
            //     the database. Therefore, it may possess a delay for the role update depending on implementation. This delay is to
            //     allow database to commit all necessary values before perform test check.
            Assert.Equal(NodeRole.Follower , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                nodeBecomeCandidateTcs.SetResult();
                node.SetElectionTimeoutInterval(int.MaxValue);
                return Task.CompletedTask;
            }
        }

        async Task PerformTestOnLeaderNode()
        {
            InitTraces(
                    $"{nameof(HandleVoteRequest_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower)}_OnLeaderNode" , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            TaskCompletionSource nodeBecomeLeaderTcs = new  TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeLeaderTcs.Task;
            Assert.Equal(NodeRole.Leader , node.Role);
            node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,

                RequesterLastLogIndex = 0 ,
                RequesterLastLogTerm = 0 ,
            });
            await Task.Delay(200);  // Important tolerant windows. Because the append entries request has a higher term and the term update to be commited to
            //     the database. Therefore, it may possess a delay for the role update depending on implementation. This delay is to
            //     allow database to commit all necessary values before perform test check.
            Assert.Equal(NodeRole.Follower , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                nodeBecomeLeaderTcs.SetResult();
                return Task.CompletedTask;
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }
    #endregion

    #region Follower Node
    [Fact]
    private async Task StartAsync_FollowerNoHeartBeatOrVoteRequestBeforeElectionTimeout_BecomeCandidate()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            TaskCompletionSource nodeTimeoutTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            InitTraces(
                    nameof(StartAsync_FollowerNoHeartBeatOrVoteRequestBeforeElectionTimeout_BecomeCandidate) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 1 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeTimeoutTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.Stop();
            await nodeStart;
            return;


            Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                nodeTimeoutTcs.SetResult();
                return Task.CompletedTask;
            }
        }
    }
    #endregion

    #region Candidate Node
    [Fact]
    private async Task StartAsync_CandidateWhenAllVoteGranted_BecomeLeader()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<bool> eventNotifyChannel = Channel.CreateUnbounded<bool>();
            InitTraces(
                    nameof(StartAsync_CandidateWhenAllVoteGranted_BecomeLeader) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Leader , node.Role);
            node.Stop();
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
                => await eventNotifyChannel.Writer.WriteAsync(true);

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });

                await eventNotifyChannel.Writer.WriteAsync(true);
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenMajorVoteGranted_BecomeLeader()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<bool> eventNotifyChannel = Channel.CreateUnbounded<bool>();
            InitTraces(
                    nameof(StartAsync_CandidateWhenMajorVoteGranted_BecomeLeader) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 5 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.AppendEntriesToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Leader , node.Role);
            node.Stop();
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
                => await eventNotifyChannel.Writer.WriteAsync(true);

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });

                await eventNotifyChannel.Writer.WriteAsync(true);
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenMinorVoteGranted_StartNewElection()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 4) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<bool> eventNotifyChannel = Channel.CreateUnbounded<bool>();
            InitTraces(
                    nameof(StartAsync_CandidateWhenMinorVoteGranted_StartNewElection) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 5 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.Stop();
            await nodeStart;
            return;

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                await node.VoteRequestReplyChannel.Writer.WriteAsync(new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                    ReplierId = s_node2Guid ,
                });

                await eventNotifyChannel.Writer.WriteAsync(true);
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenNoVoteResponse_StartNewElection()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 4) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<bool> eventNotifyChannel = Channel.CreateUnbounded<bool>();
            InitTraces(
                    nameof(StartAsync_CandidateWhenNoVoteResponse_StartNewElection) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 5 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.Stop();
            await nodeStart;
            return;

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
                => await eventNotifyChannel.Writer.WriteAsync(true);
        }
    }

    [Fact]
    private async Task StartAsync_CandidateAppendEntriesReceivedFromLeaderWithSameTerm_BecomeFollower()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2) * 2);

        testOutput.WriteLine("Test over candidate node:");
        Task test = PerformTest();
        Task completedTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completedTask , OverTimeMessage);
        await test;

        return;


        async Task PerformTest()
        {
            InitTraces(
                    $"{nameof(StartAsync_CandidateAppendEntriesReceivedFromLeaderWithSameTerm_BecomeFollower)}_OnCandidateNode" , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            TaskCompletionSource nodeBecomeCandidateTcs = new  TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,

                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                LeaderCommit = 0 ,
                Entries = [] ,
            });
            await Task.Delay(100);  // Important tolerant windows. Because the append entries request has a higher term and the term update to be commited to
            //     the database. Therefore, it may possess a delay for the role update depending on implementation. This delay is to
            //     allow database to commit all necessary values before perform test check.
            Assert.Equal(NodeRole.Follower , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                nodeBecomeCandidateTcs.SetResult();
                node.SetElectionTimeoutInterval(int.MaxValue);
                return Task.CompletedTask;
            }
        }
    }
    #endregion

    #region Leader Node
    private async Task StartAsync_LeaderAfterInit_SendAppendEntriesToOtherNodes()
        => throw new NotImplementedException();

    private async Task StartAsync_LeaderReceivedAppendEntriesFailed_DecrementNextIndex()
        => throw new NotImplementedException();

    private async Task StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueInCurrentTerm_SetCommitIndexToThatValue()
        => throw new NotImplementedException();

    private async Task StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueButNotInCurrentTerm_DoNothing()
        => throw new NotImplementedException();
    #endregion
    #endregion

    #region Restart With/Without Existing Data
    [Fact]
    private async Task ConstructorInit_RestartWithKeepExistData_RaftRestartAsFollowerNodeWithExistData()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            VoteRequestReply voteReply;

            InitTraces(
                    nameof(ConstructorInit_RestartWithKeepExistData_RaftRestartAsFollowerNodeWithExistData) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            // run node for initial data generation
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task round1 = node.StartAsync();
            AppendEntriesReply reply = node.HandleAppendEntries(new AppendEntriesArgs
            {
                RequestId = Guid.NewGuid() ,
                ReceiverId = node.NodeId ,
                RequesterId = s_node2Guid ,
                RequesterTerm = 2 ,

                LeaderCommit = 2 ,
                PreviousLogIndex = 0 ,
                PreviousLogTerm = 0 ,
                Entries = [new LogEntry()] ,
            });
            Assert.True(reply.AppendSuccess);
            await Task.Delay(200);
            Assert.Equal(2 , node.CurrentTerm);
            Assert.Equal(2 , node.LogEntries.Count);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // run node for initial data generation
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task round1 = node.StartAsync();
            reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,
                ReceiverId = node.NodeId ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
            });
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task round2 = node.StartAsync();
            reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm ,
                ReceiverId = node.NodeId ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
            });
            Assert.True(reply.VoteGranted);
            reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                RequesterId = s_node3Guid ,
                RequesterTerm = node.CurrentTerm ,
                ReceiverId = node.NodeId ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
            });
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

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval + HeartBeatInterval * 2) * 3 * 2);
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

            // run node for initial data generation
            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , HeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.AppendEntriesToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round1 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , HeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.AppendEntriesToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round2 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round2;

            // round three to double-check if it works
            node = new RaftNode(
                    s_node1Guid , ElectionTimeoutInterval , HeartBeatInterval , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            node.SendVoteRequestToOtherNodes += CreateSendVoteRequestToOtherNodesFunc(node);
            node.AppendEntriesToOtherNodes += CreateAppendEntriesToOtherNodesFunc(node);
            Assert.Equal(0 , node.CurrentTerm);
            Assert.Empty(node.LogEntries);
            Task round3 = node.StartAsync();
            await entriesAppended.Reader.ReadAsync();
            await entriesAppended.Reader.ReadAsync();
            node.Stop();
            await round3;

            return;


            Func<Guid , Guid , int , int , Task> CreateSendVoteRequestToOtherNodesFunc(RaftNode nodeIn)
                => async (Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm) =>
                {
                    VoteRequestReply replyTemplate = new VoteRequestReply
                    {
                        RequestId = requestId ,
                        ReceiverId = requesterId ,
                        ReplierTerm = node.CurrentTerm ,

                        TermOfRequest = node.CurrentTerm ,
                        VoteGranted = true ,
                    };
                    await nodeIn.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                    await nodeIn.VoteRequestReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
                };

            Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task> CreateAppendEntriesToOtherNodesFunc(RaftNode nodeIn)
                => async (Guid requestId , Guid requesterId , int commitIndex ,
                          IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes) =>
                {
                    AppendEntriesReply replyTemplate = new AppendEntriesReply
                    {
                        RequestId = requestId ,
                        ReceiverId = requesterId ,
                        ReplierTerm = node.CurrentTerm ,

                        AppendSuccess = true ,
                        MatchIndex = logEntries.Count - 1 ,
                    };
                    await nodeIn.AppendEntriesReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                    await nodeIn.AppendEntriesReplyChannel.Writer.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
                    await entriesAppended.Writer.WriteAsync(true);
                };
        }
    }

    [Fact]
    private async Task ConstructorInit_RestartWithRemoveExistData_RaftRestartWithoutVoteInfoRestored()
    {
        Task waitUnitTestTimeout = Task.Delay(2000);
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

            // run node for initial data generation
            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task round1 = node.StartAsync();
            reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                RequesterId = s_node2Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,
                ReceiverId = node.NodeId ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
            });
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round1;

            // round two for actual test
            node = new RaftNode(
                    s_node1Guid , int.MaxValue , int.MaxValue , 3 ,
                    removeExistData: true , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
                );
            Task round2 = node.StartAsync();
            reply = node.HandleVoteRequest(new VoteRequestArgs
            {
                RequestId = Guid.NewGuid() ,
                RequesterId = s_node3Guid ,
                RequesterTerm = node.CurrentTerm + 1 ,
                ReceiverId = node.NodeId ,
                RequesterLastLogIndex = node.LogEntries.Count - 1 ,
                RequesterLastLogTerm = node.LogEntries[^1].Term ,
            });
            Assert.True(reply.VoteGranted);
            node.Stop();
            await round2;
        }
    }
    #endregion

    #region Helper Function
    private void InitTraces(string testName , string? parameter , out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners)
    {
        string fileName = $"{testName}{(parameter is null ? "" : $"_{parameter}")}.log";
        standardTraceListeners =
        [
            new AlignedTraceListener($"logs/{fileName}") ,
        ];
        debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener($"debug/{fileName}") ,
        ];
    }
    #endregion
}
