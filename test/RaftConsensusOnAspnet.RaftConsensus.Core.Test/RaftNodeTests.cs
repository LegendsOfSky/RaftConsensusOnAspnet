using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers;
using System.Diagnostics;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using Xunit.Abstractions;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

public class RaftNodeTests
{
    private const int MaxTimeDeviation = 500;

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
    private async Task RaftUnitTest_ElectionTimeoutInit_StartElectionAfterNodeStartAtSpecificInterval(int electionInterval)
    {
        Task waitUnitTestTimeout = Task.Delay(electionInterval * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            TaskCompletionSource nodeBecomeElectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/ElectionTimeoutInit_{electionInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/ElectionTimeoutInit_{electionInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , electionInterval , int.MaxValue , 1 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_ElectionTimeoutChangedAtFollowerPhase_UseNewTimerToWaitForHeartBeatSignal(int electionInterval)
    {
        const int DelayBeforeElectionTimeoutChanged = 200;

        Task waitUnitTestTimeout = Task.Delay((DelayBeforeElectionTimeoutChanged + electionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            TaskCompletionSource nodeBecomeElectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/ElectionTimeoutChangedAtFollowerPhase_{electionInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/ElectionTimeoutChangedAtFollowerPhase_{electionInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , int.MaxValue  , int.MaxValue , 1 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_ElectionTimeoutChangedAtCandidatePhase_UseNewTimerToWaitForResponseBeforeTimeout(int electionInterval)
    {
        const int InitialElectionInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            Channel<DateTime> voteRequestTimeStamp = Channel.CreateUnbounded<DateTime>();
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/ElectionTimeoutChangedAtCandidatePhase_{electionInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/ElectionTimeoutChangedAtCandidatePhase_{electionInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval  , int.MaxValue , 1 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_ElectionTimeoutChangedAtLeaderPhase_DoNothing(int electionInterval)
    {
        const int InitialElectionInterval = 500;
        const int HeartBeatInterval = 1000;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + electionInterval + HeartBeatInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/ElectionTimeoutChangedAtLeaderPhase_{electionInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/ElectionTimeoutChangedAtLeaderPhase_{electionInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval , HeartBeatInterval , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_HeartBeatIntervalChangedAtFollowerPhase_DoNothing(int heartBeatInterval)
    {
        const int ElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionInterval + ElectionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/HeartBeatIntervalChangedAtFollowerPhase_{heartBeatInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/HeartBeatIntervalChangedAtFollowerPhase_{heartBeatInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_HeartBeatIntervalChangedAtCandidatePhase_DoNothing(int heartBeatInterval)
    {
        const int ElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((ElectionInterval + ElectionInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/HeartBeatIntervalChangedAtCandidatePhase_{heartBeatInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/HeartBeatIntervalChangedAtCandidatePhase_{heartBeatInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , ElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
    private async Task RaftUnitTest_HeartBeatIntervalChangedAtLeaderPhase_RestartHeartBeatTimerAndSendHeartBeatAfterNewTimerEnd(int heartBeatInterval)
    {
        const int InitialElectionInterval = 500;
        const int InitialHeartBeatInterval = 500;

        Task waitUnitTestTimeout = Task.Delay((InitialElectionInterval + InitialElectionInterval + heartBeatInterval) * 2);
        Task test = PerformTest();
        Task completeTask = await Task.WhenAny(waitUnitTestTimeout , test);
        Assert.Equal(test , completeTask);
        return;


        async Task PerformTest()
        {
            Channel<DateTime> timestamp = Channel.CreateUnbounded<DateTime>();
            TraceListener[] standardTraceListeners =
            [
                new AlignedTraceListener($"logs/HeartBeatIntervalChangedAtLeaderPhase_{heartBeatInterval}.log") ,
            ];
            TraceListener[] debugTraceListeners =
            [
                new XUnitTraceListener(testOutput) ,
                new AlignedTraceListener($"debug/HeartBeatIntervalChangedAtLeaderPhase_{heartBeatInterval}.log") ,
            ];

            RaftNode node = new RaftNode(
                    s_node1Guid , InitialElectionInterval , InitialHeartBeatInterval , 3 ,
                    removeExistData: false , standardTraceListenersIn: standardTraceListeners , debugTraceListenersIn: debugTraceListeners
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
}
