using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.UnitTests.RaftNodeTest;

// ReSharper disable once InconsistentNaming
[Collection(nameof(RaftNodeTest_SetHeartBeatInterval))]
[CollectionDefinition($"{nameof(RaftNode.SetElectionTimeoutInterval)} test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTest_SetHeartBeatInterval : RaftNodeTest_Base
{
    private const string InitialGuidFragment = "3dcbb5e5";


    public RaftNodeTest_SetHeartBeatInterval(ITestOutputHelper testOutputIn) : base(testOutputIn) { }


    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetHeartBeatInterval_HeartBeatIntervalChangedAtFollowerPhase_DoNothing(int heartBeatInterval)
    {
        const int ElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay(
                (ElectionInterval + ElectionInterval + ExtendedPeriod) * OvertimeFactor ,
                TestContext.Current.CancellationToken
            );
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

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-bf66-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = ElectionInterval ,
                        HeartBeatInterval = InitialHeartBeatInterval ,
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

        Task waitUnitTestTimeout = Task.Delay(
                (ElectionInterval * 2 + InitialHeartBeatInterval + ExtendedPeriod) * OvertimeFactor ,
                TestContext.Current.CancellationToken
            );
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

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-6f8f-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = ElectionInterval ,
                        HeartBeatInterval = InitialHeartBeatInterval ,
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

        Task waitUnitTestTimeout = Task.Delay(
                (InitialElectionInterval + InitialElectionInterval + heartBeatInterval + ExtendedPeriod) * OvertimeFactor ,
                TestContext.Current.CancellationToken
            );
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

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-27f4-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = InitialElectionInterval ,
                        HeartBeatInterval = InitialHeartBeatInterval ,
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
                Guid node2Id = Guid.Parse($"{InitialGuidFragment}-27f4-0000-0000-000000000002");
                Guid node3Id = Guid.Parse($"{InitialGuidFragment}-27f4-0000-0000-000000000003");
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node2Id });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node3Id });
            }
        }
    }
}
