using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Miscs;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

[Collection(nameof(RaftNodeTests))]
[CollectionDefinition("Base test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTests
{
    protected const int DatabaseCommitBufferPeriod = 200;
    protected const int ExtendedPeriod = 500;
    protected const int MaxTimeDeviation = 500;
    protected const int OvertimeFactor = 2;
    protected const int RegularTestTimeout = 1000;
    protected const int TestEvaluateWindow = 100;
    protected const string OverTimeMessage = "Test overtime";

    private static readonly Guid s_node1Guid = Guid.Parse("00000000-0000-0000-0000-000000000001");
    private static readonly Guid s_node2Guid = Guid.Parse("00000000-0000-0000-0000-000000000002");
    private static readonly Guid s_node3Guid = Guid.Parse("00000000-0000-0000-0000-000000000003");
    private static readonly Guid s_node4Guid = Guid.Parse("00000000-0000-0000-0000-000000000004");
    private static readonly Guid s_node5Guid = Guid.Parse("00000000-0000-0000-0000-000000000005");

    private readonly ITestOutputHelper testOutput;


    public RaftNodeTests(ITestOutputHelper testOutputIn) => testOutput = testOutputIn;


    #region Test on raft rules for each Raft role
    #region Common part for all raft roles
    [Fact]
    private async Task HandleAppendEntries_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower()
    {
        const int ElectionTimeoutInterval = 500;
        const int MaxTestTimeForCandidateNodeTest = (ElectionTimeoutInterval + DatabaseCommitBufferPeriod + ExtendedPeriod) * OvertimeFactor;
        const int MaxTestTimeForLeaderNodeTest = (ElectionTimeoutInterval * 2 + DatabaseCommitBufferPeriod + ExtendedPeriod) * OvertimeFactor;

        Task test , completedTask;

        Task waitUnitTestTimeout = Task.Delay(MaxTestTimeForCandidateNodeTest + MaxTestTimeForLeaderNodeTest , TestContext.Current.CancellationToken);

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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        LeaderCommit = 0 ,
                        Entries = [] ,
                    }
                );
            await Task.Delay(DatabaseCommitBufferPeriod);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeLeaderTcs.Task;
            Assert.Equal(NodeRole.Leader , node.Role);
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        LeaderCommit = 0 ,
                        Entries = [] ,
                    }
                );
            await Task.Delay(DatabaseCommitBufferPeriod);
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
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task HandleVoteRequest_RequesterTermLargerThanRequesteeTerm_AdvanceToRequesterTermAndBecomeFollower()
    {
        const int ElectionTimeoutInterval = 500;
        const int MaxTestTimeForCandidateNodeTest = (ElectionTimeoutInterval + DatabaseCommitBufferPeriod + ExtendedPeriod) * OvertimeFactor;
        const int MaxTestTimeForLeaderNodeTest = (ElectionTimeoutInterval * 2 + DatabaseCommitBufferPeriod + ExtendedPeriod) * OvertimeFactor;

        Task test , completedTask;

        Task waitUnitTestTimeout = Task.Delay(MaxTestTimeForCandidateNodeTest + MaxTestTimeForLeaderNodeTest , TestContext.Current.CancellationToken);

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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        RequesterLastLogIndex = 0 ,
                        RequesterLastLogTerm = 0 ,
                    }
                );
            await Task.Delay(DatabaseCommitBufferPeriod);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeLeaderTcs.Task;
            Assert.Equal(NodeRole.Leader , node.Role);
            node.HandleVoteRequest(
                    new VoteRequestArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = node.CurrentTerm + 1 ,

                        RequesterLastLogIndex = 0 ,
                        RequesterLastLogTerm = 0 ,
                    }
                );
            await Task.Delay(DatabaseCommitBufferPeriod);
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
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }
    #endregion

    #region Follower Node
    [Fact]
    private async Task StartAsync_FollowerNoHeartBeatOrVoteRequestBeforeElectionTimeout_BecomeCandidate()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
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
                await eventNotifyChannel.Writer.WriteAsync(true);
                await Task.Delay(TestEvaluateWindow);
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenMajorVoteGranted_BecomeLeader()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 5 ,
                        RemoveExistData = true ,
                    } ,
                    new RaftNodeTraces
                    {
                        StandardTraceListeners = standardTraceListeners ,
                        DebugTraceListeners = debugTraceListeners ,
                    }
                );
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
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
                await eventNotifyChannel.Writer.WriteAsync(true);
                await Task.Delay(TestEvaluateWindow);
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenMinorVoteGranted_StartNewElection()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 3 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 5 ,
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
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            await eventNotifyChannel.Reader.ReadAsync();
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.Stop();
            await nodeStart;
            return;

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                await eventNotifyChannel.Writer.WriteAsync(true);
                await Task.Delay(TestEvaluateWindow);
                await node.VoteRequestReplyChannelWriter.WriteAsync(
                        new VoteRequestReply
                        {
                            RequestId = requestId ,
                            TermOfRequest = node.CurrentTerm ,
                            ReceiverId = requestId ,
                            ReplierTerm = node.CurrentTerm ,
                            VoteGranted = true ,
                            ReplierId = s_node2Guid ,
                        }
                    );
            }
        }
    }

    [Fact]
    private async Task StartAsync_CandidateWhenNoVoteResponse_StartNewElection()
    {
        const int ElectionTimeoutInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 3 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
                        HeartBeatInterval = int.MaxValue ,
                        NodeCount = 5 ,
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

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);

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
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
                        ElectionTimeOutInterval = ElectionTimeoutInterval ,
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
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await nodeBecomeCandidateTcs.Task;
            Assert.Equal(NodeRole.Candidate , node.Role);
            node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        ReceiverId = node.NodeId ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = node.CurrentTerm ,

                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        LeaderCommit = 0 ,
                        Entries = [] ,
                    }
                );
            await Task.Delay(DatabaseCommitBufferPeriod);
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
    [Fact]
    private async Task StartAsync_LeaderAfterInit_SendAppendEntriesToOtherNodes()
    {
        const int ElectionTimeoutInterval = 500;
        const int HeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + HeartBeatInterval + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            TaskCompletionSource appendEntriesRequestSentTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            InitTraces(
                    nameof(StartAsync_LeaderAfterInit_SendAppendEntriesToOtherNodes) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            await appendEntriesRequestSentTcs.Task;
            Assert.Equal(NodeRole.Leader , node.Role);

            node.Stop();
            await nodeStart;
            return;


            Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                appendEntriesRequestSentTcs.SetResult();
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
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task StartAsync_LeaderReceivedAppendEntriesFailed_DecrementNextIndex()
    {
        const int ElectionTimeoutInterval = 500;
        const int HeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + HeartBeatInterval * 3 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            IReadOnlyDictionary<Guid , int> nextIndexes;

            Channel<IReadOnlyDictionary<Guid , int>> nextIndexesFromNode = Channel.CreateUnbounded<IReadOnlyDictionary<Guid , int>>();
            Channel<bool> appendRequestSuccessOrNot = Channel.CreateUnbounded<bool>();

            InitTraces(
                    nameof(StartAsync_LeaderReceivedAppendEntriesFailed_DecrementNextIndex) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            AppendEntriesReply appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = 1 ,
                        ReceiverId = node.NodeId ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test1" , 1) ,
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test2" , 2) ,
                        ] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect 2 test entry to be appended to the node, but failed.");
            Assert.Equal(3 , node.LogEntries.Count);  // Total log entries count should be 2 newly appended and an initial entry.
            nextIndexes = await nextIndexesFromNode.Reader.ReadAsync();  // wait until test node become leader
            foreach ((_ , int nextIndex) in nextIndexes)
                Assert.Equal(3 , nextIndex);
            await appendRequestSuccessOrNot.Writer.WriteAsync(false);
            nextIndexes = await nextIndexesFromNode.Reader.ReadAsync();
            foreach ((_ , int nextIndex) in nextIndexes.Where(nextIndex => nextIndex.Key != Guid.Empty))
                Assert.Equal(2 , nextIndex);

            node.Stop();
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexesIn)
            {
                await nextIndexesFromNode.Writer.WriteAsync(nextIndexesIn);

                bool appendSuccess = await appendRequestSuccessOrNot.Reader.ReadAsync();
                AppendEntriesReply replyTemplate = new AppendEntriesReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,

                    AppendSuccess = appendSuccess ,
                    MatchIndex = 0 ,
                };
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueInCurrentTerm_SetCommitIndexToThatValue()
    {
        const int ElectionTimeoutInterval = 500;
        const int HeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + HeartBeatInterval * 3 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<int> commitIndexChannel = Channel.CreateUnbounded<int>();
            InitTraces(
                    nameof(StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueInCurrentTerm_SetCommitIndexToThatValue) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            AppendEntriesReply appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = 1 ,
                        ReceiverId = node.NodeId ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test1" , 1) ,
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test2" , 2) ,
                        ] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect 2 test entry to be appended to the node, but failed.");
            Assert.Equal(3 , node.LogEntries.Count);  // Total log entries count should be 2 newly appended and an initial entry.
            Assert.Equal(0 , await commitIndexChannel.Reader.ReadAsync());
            Task propose = node.ProposeAsync(LogEntryOperation.Put , "test3" , 1);
            Assert.Equal(0 , await commitIndexChannel.Reader.ReadAsync());
            Assert.Equal(3 , await commitIndexChannel.Reader.ReadAsync());

            await propose;
            node.Stop();
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                await commitIndexChannel.Writer.WriteAsync(commitIndex);
                AppendEntriesReply replyTemplate = new AppendEntriesReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,

                    AppendSuccess = true ,
                    MatchIndex = node.LogEntries.Count - 1 ,
                };
                await Task.Delay(TestEvaluateWindow);
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }

    [Fact]
    private async Task StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueButNotInCurrentTerm_DoNothing()
    {
        const int ElectionTimeoutInterval = 500;
        const int HeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionTimeoutInterval * 2 + HeartBeatInterval * 3 + ExtendedPeriod) * OvertimeFactor , TestContext.Current.CancellationToken);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.True(test == completeTask , OverTimeMessage);
        await test;
        return;


        async Task PerformTest()
        {
            Channel<int> commitIndexChannel = Channel.CreateUnbounded<int>();
            InitTraces(
                    nameof(StartAsync_LeaderReceivedMajorityOfMatchIndexLargerThanAnValueButNotInCurrentTerm_DoNothing) , null ,
                    out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners
                );

            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = s_node1Guid ,
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
            node.SendAppendEntriesRequestsToOtherNodes += AppendEntriesToOtherNodes;
            node.SendVoteRequestToOtherNodes += SendVoteRequestToOtherNodes;

            Task nodeStart = node.StartAsync();
            AppendEntriesReply appendEntriesReply = node.HandleAppendEntries(
                    new AppendEntriesArgs
                    {
                        RequestId = Guid.NewGuid() ,
                        RequesterId = s_node2Guid ,
                        RequesterTerm = 1 ,
                        ReceiverId = node.NodeId ,

                        LeaderCommit = 0 ,
                        PreviousLogIndex = 0 ,
                        PreviousLogTerm = 0 ,
                        Entries =
                        [
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test1" , 1) ,
                            new Int32LogEntry(Guid.NewGuid() , 1 , LogEntryOperation.Put , "test2" , 2) ,
                        ] ,
                    }
                );
            Assert.True(appendEntriesReply.AppendSuccess , "Expect 2 test entry to be appended to the node, but failed.");
            Assert.Equal(3 , node.LogEntries.Count);  // Total log entries count should be 2 newly appended and an initial entry.
            Assert.Equal(0 , await commitIndexChannel.Reader.ReadAsync());
            Assert.Equal(0 , await commitIndexChannel.Reader.ReadAsync());

            node.Stop();
            await nodeStart;
            return;


            async Task AppendEntriesToOtherNodes(
                Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
            {
                await commitIndexChannel.Writer.WriteAsync(commitIndex);
                await Task.Delay(TestEvaluateWindow);
                AppendEntriesReply replyTemplate = new AppendEntriesReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,

                    AppendSuccess = true ,
                    MatchIndex = 2 ,
                };
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.AppendEntriesReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }

            async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm)
            {
                VoteRequestReply replyTemplate = new VoteRequestReply
                {
                    RequestId = requestId ,
                    ReplierTerm = node.CurrentTerm ,
                    ReceiverId = requestId ,
                    TermOfRequest = node.CurrentTerm ,
                    VoteGranted = true ,
                };
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node2Guid });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = s_node3Guid });
            }
        }
    }
    #endregion
    #endregion

    #region Helper Function
    protected void InitTraces(string testName , string? parameter , out TraceListener[] standardTraceListeners , out TraceListener[] debugTraceListeners)
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
