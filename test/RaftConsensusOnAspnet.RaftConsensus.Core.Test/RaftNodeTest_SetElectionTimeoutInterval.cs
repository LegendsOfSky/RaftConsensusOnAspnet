using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using Xunit.Abstractions;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

// ReSharper disable once InconsistentNaming
[Collection(nameof(RaftNodeTest_SetElectionTimeoutInterval))]
[CollectionDefinition($"{nameof(RaftNode.SetElectionTimeoutInterval)} test for RaftConsensusOnAspnet.RaftConsensus.Core.RaftNode")]
public class RaftNodeTest_SetElectionTimeoutInterval : RaftNodeTests
{
    private const string InitialGuidFragment = "6c320080";


    public RaftNodeTest_SetElectionTimeoutInterval(ITestOutputHelper testOutputIn) : base(testOutputIn) { }


    [Theory]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    [InlineData(2500)]
    private async Task SetElectionTimeoutInterval_ElectionTimeoutChangedAtFollowerPhase_UseNewTimerToWaitForHeartBeatSignal(int electionInterval)
    {
        const int DelayBeforeElectionTimeoutChanged = 200;

        Task waitUnitTestTimeout = Task.Delay((DelayBeforeElectionTimeoutChanged + electionInterval + ExtendedPeriod) * OvertimeFactor);
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

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-b568-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = int.MaxValue ,
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

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval + ExtendedPeriod) * OvertimeFactor);
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

            Guid nodeId = Guid.Parse($"{InitialGuidFragment}-b5df-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = nodeId ,
                        ElectionTimeOutInterval = InitialElectionInterval ,
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
        const string SecondGuidFragment = "39a9";

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval + HeartBeatInterval * 2 + ExtendedPeriod) * OvertimeFactor);
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

            Guid node1Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000001");
            RaftNode node = new RaftNode(
                    new RaftNodeOptions
                    {
                        NodeId = node1Id ,
                        ElectionTimeOutInterval = InitialElectionInterval ,
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
                Guid node2Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000002");
                Guid node3Id = Guid.Parse($"{InitialGuidFragment}-{SecondGuidFragment}-0000-0000-000000000003");
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node2Id });
                await node.VoteRequestReplyChannelWriter.WriteAsync(replyTemplate with { ReplierId = node3Id });
            }
        }
    }
}
