using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;
using System.Diagnostics;
using System.Text;
using RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers;
using Xunit.Abstractions;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test;

public class RaftNodeMultiNodeTests
{
    private const int MaxTestTime = 30000;
    private const string MaxTestTimeExceedMsg = "Test failed: overtimed.";
    private readonly ITestOutputHelper testOutput;


    public RaftNodeMultiNodeTests(ITestOutputHelper output) => testOutput = output;


    [Fact]
    private async Task RaftClusterTest_01LeaderElection_01OneCandidateOneRoundElection()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_01OneCandidateOneRoundElection.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_01OneCandidateOneRoundElection.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_01OneCandidateOneRoundElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_01OneCandidateOneRoundElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_01OneCandidateOneRoundElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_01OneCandidateOneRoundElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_01OneCandidateOneRoundElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_01OneCandidateOneRoundElection" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true , MessageDropped = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_01LeaderElection_02OneCandidateStartTwoElection()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_02OneCandidateStartTwoElection.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_02OneCandidateStartTwoElection.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_02OneCandidateStartTwoElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_02OneCandidateStartTwoElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_02OneCandidateStartTwoElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_02OneCandidateStartTwoElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_02OneCandidateStartTwoElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_02OneCandidateStartTwoElection" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(3000);
            nodes[4].SetElectionTimeoutInterval(3000);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_01LeaderElection_03TwoCandidateForElection()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_03TwoCandidateForElection.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_03TwoCandidateForElection.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_03TwoCandidateForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_03TwoCandidateForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_03TwoCandidateForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_03TwoCandidateForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_03TwoCandidateForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_03TwoCandidateForElection" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(2000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(4000);
            nodes[4].SetElectionTimeoutInterval(4000);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , MessageDropped = true });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[2].NodeId });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[2].NodeId });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[2].NodeId });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[2].NodeId });
            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_01LeaderElection_04SplitVote()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_04SplitVote.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_04SplitVote.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_04SplitVote" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_04SplitVote" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_04SplitVote" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_04SplitVote" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_04SplitVote" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_04SplitVote" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(2000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(3000);
            await Task.Delay(2100);
            nodes[3].SetElectionTimeoutInterval(300);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            // T = 2000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });

            // T = 2000
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 1500 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 1500 });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 1500 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 1500 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 1500 });

            // T = 2000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true , MessageDropped = true });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            // T = 2400 (node 3 election restart)
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , LeaderId = nodes[3].NodeId , Entries = [] });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , LeaderId = nodes[3].NodeId , Entries = [] });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , LeaderId = nodes[3].NodeId , Entries = [] });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , LeaderId = nodes[3].NodeId , Entries = [] });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true , MatchIndex = 0 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true , MatchIndex = 0 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true , MatchIndex = 0 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true , MatchIndex = 0 });

            // T = 2900
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_01LeaderElection_05AllForElection()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_05AllForElection.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_05AllForElection.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_05AllForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_05AllForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_05AllForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_05AllForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_05AllForElection" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_05AllForElection" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(1000);
            nodes[2].SetElectionTimeoutInterval(1000);
            nodes[3].SetElectionTimeoutInterval(1000);
            nodes[4].SetElectionTimeoutInterval(1000);
            await Task.Delay(1500);
            nodes[0].SetElectionTimeoutInterval(5000);
            nodes[1].SetElectionTimeoutInterval(5000);
            nodes[2].SetElectionTimeoutInterval(5000);
            nodes[3].SetElectionTimeoutInterval(5000);
            nodes[4].SetElectionTimeoutInterval(1000);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            // T = 1000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });

            // T = 1000
            await connections[1 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[1 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[1 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[1 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });

            // T = 1000
            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });

            // T = 1000
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });

            // T = 1000
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 500 });

            // T = 1500
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });

            // T = 1500
            await connections[1 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[1 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[1 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[1 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });

            // T = 1500
            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });

            // T = 1500
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });

            // T = 1500
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500 });

            // T = 2000
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });

            // T = 2000
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            // T = 2000
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_01LeaderElection_06LeaderRevertToFollower()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/01LeaderElection_06LeaderRevertToFollower.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/01LeaderElection_06LeaderRevertToFollower.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/01LeaderElection_06LeaderRevertToFollower" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/01LeaderElection_06LeaderRevertToFollower" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/01LeaderElection_06LeaderRevertToFollower" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/01LeaderElection_06LeaderRevertToFollower" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/01LeaderElection_06LeaderRevertToFollower" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("01LeaderElection_06LeaderRevertToFollower" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
            await Task.Delay(1100);
            nodes[4].SetElectionTimeoutInterval(500);
            return (true , "");
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            // T = 1000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            // T = 1000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            // T = 1400
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 , MessageDropped = true });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 , MessageDropped = true });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            // T = 1400
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[4].NodeId });
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_02LogReplication_01OneSimplePut()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/02LogReplication_01OneSimplePut.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/02LogReplication_01OneSimplePut.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/02LogReplication_01OneSimplePut" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/02LogReplication_01OneSimplePut" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/02LogReplication_01OneSimplePut" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/02LogReplication_01OneSimplePut" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/02LogReplication_01OneSimplePut" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("02LogReplication_01OneSimplePut" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder) = (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(3000);
            nodes[4].SetElectionTimeoutInterval(3000);
            nodes[0].SetHeartBeatInterval(2000);

            await Task.Delay(2000);
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] proposeTasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];
            if (await Task.WhenAny(watchDogTimer , Task.WhenAll(proposeTasks)) == watchDogTimer)
                return (false , MaxTestTimeExceedMsg);

            /* Verify propose. */
            await Task.Delay(4000);
            if (await proposeTasks[0] is not { Success: true, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair to node 0 failed. "
                        + string.Format(
                                "Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await proposeTasks[0]).Success , (await proposeTasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await proposeTasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            $"Proposing new key-value pair to node {i} failed. Expect: Success = false, WrongNode = true. "
                            + string.Format(
                                    "but get: Success = {0}, WrongNode = {1}\n" ,
                                    (await proposeTasks[i]).Success , (await proposeTasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            for (int i = 0; i < nodes.Length; i++)
            {
                const int ExpectValue = 1;

                (bool success , bool keyFound , object? value) = nodes[i].GetValue("test");
                if (!success || !keyFound || !Equals(value , ExpectValue))
                {
                    debugMsgBuilder.Append(
                            $"Get value from node {i} failed. Expect: Success = True, KeyFound = True, Value = 1. "
                            + string.Format(
                                    "but get: Success = {0}, KeyFound = {1}, Value = {2}\n" ,
                                    success , keyFound , Equals(value , ExpectValue) ? ExpectValue : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            connections = await ConfigureNetworkForLeaderElectionAsync(connections , nodes[0]);

            LogEntry entry = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_02LogReplication_02OneSimpleUpdate()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/02LogReplication_02OneSimpleUpdate.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/02LogReplication_02OneSimpleUpdate.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/02LogReplication_02OneSimpleUpdate" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/02LogReplication_02OneSimpleUpdate" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/02LogReplication_02OneSimpleUpdate" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/02LogReplication_02OneSimpleUpdate" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/02LogReplication_02OneSimpleUpdate" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("02LogReplication_02OneSimpleUpdate" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder) = (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(6000);
            nodes[2].SetElectionTimeoutInterval(6000);
            nodes[3].SetElectionTimeoutInterval(6000);
            nodes[4].SetElectionTimeoutInterval(6000);
            nodes[0].SetHeartBeatInterval(2000);

            await Task.Delay(2000);
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];

            await Task.Delay(2000);
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(4000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 0 failed. "
                        + string.Format(
                                "Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose1Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            $"Proposing new key-value pair to node {i} failed. "
                            + string.Format(
                                    "Expect: Success = false, WrongNode = true. but get: Success = {0}, WrongNode = {1}\n" ,
                                    (await propose1Tasks[i]).Success , (await propose1Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Check propose 2 (Term 1: Put <test: 2>). */
            if (await propose2Tasks[0] is not { Success: true, WrongNode: false, KeyFound: true })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 2 failed. "
                        + string.Format(
                                "Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks[0]).Success , (await propose2Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose2Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            $"Proposing new key-value pair to node {i} failed. "
                            + string.Format(
                                    "Expect: Success = false, WrongNode = true. but get: Success = {0}, WrongNode = {1}\n" ,
                                    (await propose2Tasks[i]).Success , (await propose2Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Try get values for correctness check. */
            for (int i = 0; i < nodes.Length; i++)
            {
                const int ExpectValue = 2;

                (bool success , bool keyFound , object? value) = nodes[i].GetValue("test");
                if (!success || !keyFound || !Equals(value , ExpectValue))
                {
                    debugMsgBuilder.Append(
                            $"Get value from node {i} failed. "
                            + string.Format(
                                    "Expect: Success = True, KeyFound = True, Value = 1. but get: Success = {0}, KeyFound = {1}, Value = {2}\n" ,
                                    success , keyFound , Equals(value , ExpectValue) ? ExpectValue : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            connections = await ConfigureNetworkForLeaderElectionAsync(connections , nodes[0]);

            LogEntry entry1 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            LogEntry entry2 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 2);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_02LogReplication_03OneSimpleDelete()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/02LogReplication_03OneSimpleDelete.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/02LogReplication_03OneSimpleDelete.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/02LogReplication_03OneSimpleDelete" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/02LogReplication_03OneSimpleDelete" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/02LogReplication_03OneSimpleDelete" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/02LogReplication_03OneSimpleDelete" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/02LogReplication_03OneSimpleDelete" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("02LogReplication_03OneSimpleDelete" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder) = (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(8000);
            nodes[2].SetElectionTimeoutInterval(8000);
            nodes[3].SetElectionTimeoutInterval(8000);
            nodes[4].SetElectionTimeoutInterval(8000);
            nodes[0].SetHeartBeatInterval(2000);

            await Task.Delay(2000);
            const string ProposeKey = "test";
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1) ,
            ];

            await Task.Delay(2000);
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null) ,
                nodes[1].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null) ,
                nodes[2].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null) ,
                nodes[3].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null) ,
                nodes[4].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(4000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 0 failed. "
                        + string.Format(
                                "Expect: Success = True, WrongNode = False. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose1Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            $"Proposing new key-value (1) pair to node {i} failed. "
                            + string.Format(
                                    "Expect: Success = False, WrongNode = True. but get: Success = {0}, WrongNode = {1}\n" ,
                                    (await propose1Tasks[i]).Success , (await propose1Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Check propose 2 (Term 1: Delete <test: 2>). */
            if (await propose2Tasks[0] is not { Success: true, WrongNode: false, KeyFound: true })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 0 failed. "
                        + string.Format(
                                "Expect: Success = True, WrongNode = False. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks[0]).Success , (await propose2Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose2Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            $"Proposing new key-value (1) pair to node {i} failed. "
                            + string.Format(
                                    "Expect: Success = False, WrongNode = True. but get: Success = {0}, WrongNode = {1}\n" ,
                                    (await propose2Tasks[i]).Success , (await propose2Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Try get values for correctness check. */
            for (int i = 0; i < nodes.Length; i++)
            {
                (bool success , bool keyFound , object? value) = nodes[i].GetValue("test");
                if (!success || keyFound || !Equals(value , null))
                {
                    debugMsgBuilder.Append(
                            $"Get value from node {i} failed. "
                            + string.Format(
                                    "Expect: Success = True, KeyFound = False, Value = null. but get: Success = {0}, KeyFound = {1}, Value = {2}\n" ,
                                    success , keyFound , Equals(value , null) ? "null" : 1
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            connections = await ConfigureNetworkForLeaderElectionAsync(connections , nodes[0]);

            // heart beat #1
            LogEntry entry1 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            // heart beat #2
            LogEntry entry2 = new Int32LogEntry(1 , LogEntryOperation.Delete , "test" , null);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true , PreviousLogIndex = 1 , PreviousLogTerm = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            // heart beat #3
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 , PreviousLogIndex = 1 , PreviousLogTerm = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 , PreviousLogIndex = 2 , PreviousLogTerm = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            // heart beat #4
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_02LogReplication_04DeleteNonExistKey()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/02LogReplication_04DeleteNonExistKey.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/02LogReplication_04DeleteNonExistKey.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/02LogReplication_04DeleteNonExistKey" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/02LogReplication_04DeleteNonExistKey" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/02LogReplication_04DeleteNonExistKey" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/02LogReplication_04DeleteNonExistKey" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/02LogReplication_04DeleteNonExistKey" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("02LogReplication_04DeleteNonExistKey" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder) = (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(8000);
            nodes[2].SetElectionTimeoutInterval(8000);
            nodes[3].SetElectionTimeoutInterval(8000);
            nodes[4].SetElectionTimeoutInterval(8000);
            nodes[0].SetHeartBeatInterval(2000);

            await Task.Delay(2000);
            const string Propose1Key = "test";
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , Propose1Key , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , Propose1Key , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , Propose1Key , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , Propose1Key , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , Propose1Key , 1) ,
            ];

            await Task.Delay(2000);
            const string Propose2Key = "test2";
            Task<(bool Success , bool WrongNode , bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Delete , Propose2Key , null) ,
                nodes[1].ProposeAsync(LogEntryOperation.Delete , Propose2Key , null) ,
                nodes[2].ProposeAsync(LogEntryOperation.Delete , Propose2Key , null) ,
                nodes[3].ProposeAsync(LogEntryOperation.Delete , Propose2Key , null) ,
                nodes[4].ProposeAsync(LogEntryOperation.Delete , Propose2Key , null) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = await Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(2000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 0 failed. Expect: Success = true, WrongNode = false. "
                        + string.Format(
                                "but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }

            /* Check propose 2 (Term 1: Delete <test: 2>). */
            if (await propose2Tasks[0] is not { Success: false, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 2 failed. Expect: Success = true, WrongNode = false. "
                        + string.Format(
                                "but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks[0]).Success , (await propose2Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }

            /* Try get values for correctness check. */
            await Task.Delay(2000);
            for (int i = 0; i < nodes.Length; i++)
            {
                (bool success , bool keyFound , object? value) = nodes[i].GetValue("test");
                if (!success || !keyFound || !Equals(value , 1))
                {
                    debugMsgBuilder.Append(
                            $"Get value from node {i} failed. Expect: Success = True, KeyFound = True , Value = null. "
                            + string.Format(
                                    "but get: Success = {0}, KeyFound = {1}, Value = {2}\n" ,
                                    success , keyFound , Equals(value , 1) ? 1 : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            connections = await ConfigureNetworkForLeaderElectionAsync(connections , nodes[0]);

            // heart beat #1
            LogEntry entry1 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });

            // heart beat #2
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });

            // heart beat #3
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            // heart beat #4
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            return connections;
        }
    }

    [Fact]
    private async Task RaftClusterTest_03ApplyLogEntries_01StateMachineValue()
    {
        TraceListener[] standardTraceListeners =
        [
            new AlignedTraceListener("logs/03ApplyLogEntries_01StateMachineValue.log") ,
        ];
        TraceListener[] debugTraceListeners =
        [
            new XUnitTraceListener(testOutput) ,
            new AlignedTraceListener("debug/03ApplyLogEntries_01StateMachineValue.log") ,
        ];
        Dictionary<Guid , int> nodeIdToDebugPos = [];
        RaftNode[] nodes =
        [
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000001") , 10000 , 10000 , 5 , "data/03ApplyLogEntries_01StateMachineValue" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000002") , 10000 , 10000 , 5 , "data/03ApplyLogEntries_01StateMachineValue" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000003") , 10000 , 10000 , 5 , "data/03ApplyLogEntries_01StateMachineValue" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000004") , 10000 , 10000 , 5 , "data/03ApplyLogEntries_01StateMachineValue" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
            new RaftNode(Guid.Parse("00000000-0000-0000-0000-000000000005") , 10000 , 10000 , 5 , "data/03ApplyLogEntries_01StateMachineValue" , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos) ,
        ];
        Assert.True(await CreateAndRunTestCaseAsync("03ApplyLogEntries_01StateMachineValue" , nodes , ManipulateNodes , CreateConfig));
        return;


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder) = (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(4000);
            nodes[2].SetElectionTimeoutInterval(4000);
            nodes[3].SetElectionTimeoutInterval(4000);
            nodes[4].SetElectionTimeoutInterval(4000);
            nodes[0].SetHeartBeatInterval(1000);

            await Task.Delay(1500);
            const string ProposeKey = "test";

            /* Propose (Term 1: Put <test: 1>). Then check if propose success and state machine values correct. */
            Task<(bool Success , bool WrongNode , bool? KeyFound)> propose1Tasks = nodes[0].ProposeAsync(LogEntryOperation.Put , ProposeKey , 1);
            bool propose1Finished = await Task.WhenAny(watchDogTimer , propose1Tasks) != watchDogTimer;
            if (!propose1Finished)
                return (false , MaxTestTimeExceedMsg);
            if (await propose1Tasks is not { Success: true, WrongNode: false, KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. Expect: Success = true, WrongNode = false. " +
                                "but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks).Success , (await propose1Tasks).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            await Task.Delay(1200);
            foreach (RaftNode node in nodes.Where(node => node.LastAppliedLogEntryIndex != 1))
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "State machine values out of sync with log entries at node {0} propose 1 (lastApplyIndex: expect 1, get {1}).\n" ,
                                node.NodeIdToDebugPos[node.NodeId] , node.LastAppliedLogEntryIndex
                            )
                    );
                manipulationPassed = false;
            }
            CheckNodeStateMachineValues(1);

            /* Propose (Term 1: Put <test: 2>). Then check if propose success and state machine values correct. */
            Task<(bool Success , bool WrongNode , bool? KeyFound)> propose2Tasks = nodes[0].ProposeAsync(LogEntryOperation.Put , ProposeKey , 2);
            bool propose2Finished = await Task.WhenAny(watchDogTimer , propose2Tasks) != watchDogTimer;
            if (!propose2Finished)
                return (false , MaxTestTimeExceedMsg);
            if (await propose2Tasks is not { Success: true, WrongNode: false, KeyFound: true })
            {
                debugMsgBuilder.Append(
                        "Proposing new key-value pair (1) to node 0 failed. Expect: Success = true, WrongNode = false. "
                        + string.Format(
                                "but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks).Success , (await propose2Tasks).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            await Task.Delay(1200);
            foreach (RaftNode node in nodes.Where(node => node.LastAppliedLogEntryIndex != 2))
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "State machine values out of sync with log entries at node {0} propose 2 (lastApplyIndex: expect 2, get {1}).\n" ,
                                node.NodeIdToDebugPos[node.NodeId] , node.LastAppliedLogEntryIndex
                            )
                    );
                manipulationPassed = false;
            }
            CheckNodeStateMachineValues(2);

            /* Propose (Term 1: Delete <test: null>). Then check if propose success and state machine values correct. */
            Task<(bool Success , bool WrongNode , bool? KeyFound)> propose3Tasks = nodes[0].ProposeAsync(LogEntryOperation.Delete , ProposeKey , null);
            bool propose3Finished = await Task.WhenAny(watchDogTimer , propose3Tasks) != watchDogTimer;
            if (!propose3Finished)
                return (false , MaxTestTimeExceedMsg);
            if (await propose3Tasks is not { Success: true, WrongNode: false, KeyFound: true })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. "
                                + "Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose3Tasks).Success , (await propose3Tasks).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            await Task.Delay(1200);
            foreach (RaftNode node in nodes.Where(node => node.LastAppliedLogEntryIndex != 3))
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "State machine values out of sync with log entries at node {0} propose 2 (lastApplyIndex: expect 3, get {1}).\n" ,
                                node.NodeIdToDebugPos[node.NodeId] , node.LastAppliedLogEntryIndex
                            )
                    );
                manipulationPassed = false;
            }
            CheckNodeStateMachineValues(null);

            return (manipulationPassed , debugMsgBuilder.ToString());


            void CheckNodeStateMachineValues(int? expectResult)
            {
                foreach (RaftNode node in nodes)
                {
                    using SqliteConnection connection = DbHelper.CreateNewConnection($"data/03ApplyLogEntries_01StateMachineValue/{node.NodeId}.db");

                    using SqliteCommand getRecordCount = new SqliteCommand($"SELECT COUNT(*) FROM {LogEntryList.StateMachineValuesTableName};" , connection);
                    int recordCount = Convert.ToInt32(getRecordCount.ExecuteScalar());
                    int expectRecordCount = expectResult is null ? 0 : 1;
                    if (recordCount != expectRecordCount)
                    {
                        debugMsgBuilder.Append(
                                string.Format(
                                        "Unmatch record count in node {0}. Expect number of record in {1} is {2}, but get {3}\n" ,
                                        node.NodeIdToDebugPos[node.NodeId] , LogEntryList.StateMachineValuesTableName , expectRecordCount , recordCount
                                    )
                            );
                        manipulationPassed = false;
                        continue;
                    }

                    if (expectResult is not null)
                    {
                        using SqliteCommand getRecords
                            = new SqliteCommand($"SELECT Key , Value , Type FROM {LogEntryList.StateMachineValuesTableName};" , connection);
                        using SqliteDataReader recordsReader = getRecords.ExecuteReader();
                        recordsReader.Read();
                        string key   = recordsReader.GetString(0);
                        int    value = int.Parse(recordsReader.GetString(1));
                        string type  = recordsReader.GetString(2);
                        if ((key , value , type) != ("test" , expectResult , Int32LogEntry.LogType))
                        {
                            debugMsgBuilder.Append(
                                    string.Format(
                                            "Unmatch record count in node {0}. Expect (\"test\", {1}, {2}), but get (\"{3}\", {4}, \"{5}\")\n" ,
                                            node.NodeIdToDebugPos[node.NodeId] , expectResult , Int32LogEntry.LogType , key , value , type
                                        )
                                );
                            manipulationPassed = false;
                        }
                    }
                }
            }
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            connections = await ConfigureNetworkForLeaderElectionAsync(connections , nodes[0]);

            // heart beat #1 and #2
            LogEntry entry1 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            // heart beat #3 and #4
            LogEntry entry2 = new Int32LogEntry(1 , LogEntryOperation.Put , "test" , 2);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry2] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 1 , PreviousLogTerm = 1 , LeaderCommit = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            // heart beat #5 amd #6
            LogEntry entry3 = new Int32LogEntry(1 , LogEntryOperation.Delete , "test" , null);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry3] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry3] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry3] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry3] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 2 , PreviousLogTerm = 1 , LeaderCommit = 2 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 3 , PreviousLogTerm = 1 , LeaderCommit = 3 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 3 , PreviousLogTerm = 1 , LeaderCommit = 3 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 3 , PreviousLogTerm = 1 , LeaderCommit = 3 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId , PreviousLogIndex = 3 , PreviousLogTerm = 1 , LeaderCommit = 3 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 3 });

            return connections;
        }
    }

    private static async Task<NetworkConnection[,]> ConfigureNetworkForLeaderElectionAsync(NetworkConnection[,] connections , RaftNode winningNode)
    {
        await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
        await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
        await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
        await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
        await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
        await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
        await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
        await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

        await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = winningNode.NodeId });
        await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = winningNode.NodeId });
        await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = winningNode.NodeId });
        await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = winningNode.NodeId });
        await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
        await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
        await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
        await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

        return connections;
    }

    private async Task<bool> CreateAndRunTestCaseAsync(
         string testName , RaftNode[] nodes ,
        Func<Task<(bool Success , string DebugMsg)>> nodeManipulation , Func<NetworkConnection[,] , Task<NetworkConnection[,]>> configs)
    {
        testOutput.WriteLine($"Begin test {testName}");

        /* Prepare test environment. */
        NetworkConnection[,] connections = new NetworkConnection[nodes.Length , nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
        {
            for (int j = 0; j < nodes.Length; j++)
            {
                connections[i , j] = new NetworkConnection(nodes[i] , nodes[j] , testOutput);

                if (i == j)
                    continue;

                nodes[i].SendVoteRequestToOtherNodes += connections[i , j].HandleVoteRequestAsync;
                nodes[i].AppendEntriesToOtherNodes += connections[i , j].HandleAppendEntriesAsync;
            }
        }
        await configs.Invoke(connections);  // encode the correct raft node behavior

        /* Start raft nodes. */
        Task[] raftStartTasks = new Task[nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
            raftStartTasks[i] = nodes[i].StartAsync();
        (bool manipulationPassed , string manipulationDebugMsg) = await nodeManipulation.Invoke();
        Task watchDogInterrupt = Task.Delay(MaxTestTime);

        /* Wait to finalize the test case. */
        List<Task> waitForExpectMsgChannelReaderCloses = new List<Task>(connections.Length);
        foreach (NetworkConnection connection in connections)
        {
            connection.ExpectedMessagesChannel.Writer.Complete();
            waitForExpectMsgChannelReaderCloses.Add(connection.ExpectedMessagesChannel.Reader.Completion);
        }
        Task waitForAllExpectMsgChannelClose = Task.WhenAll(waitForExpectMsgChannelReaderCloses);
        Task finishedTask = await Task.WhenAny(waitForAllExpectMsgChannelClose , watchDogInterrupt);
        if (finishedTask == watchDogInterrupt)
        {
            testOutput.WriteLine(MaxTestTimeExceedMsg);
            return false;
        }
        Array.ForEach(nodes , node => node.Stop());
        await Task.WhenAll(raftStartTasks);
        Trace.Listeners.Clear();

        /* Check for any mismatch behaviour */
        bool[,] missingMessages = new bool[nodes.Length , nodes.Length];
        (DateTime firstErrorTime , int firstErrorSource) = (DateTime.Now , -1);
        for (int i = 0; i < nodes.Length; i++)
            for (int j = 0; j < nodes.Length; j++)
            {
                if (i == j)
                    continue;

                if (connections[i , j].ExpectedMessagesChannel.Reader.Count > 0)
                    missingMessages[i , j] = true;

                foreach ((MessagePackageBase expect , MessagePackageBase actual , DateTime time) in connections[i , j].DebugLogs)
                    if (expect != actual && firstErrorTime > time)
                        (firstErrorTime , firstErrorSource) = (time , i);
            }

        /* Report the final result. */
        bool hasMismatchMessage = connections.Cast<NetworkConnection>().Any(connection => connection.HasMismatchMessage);
        bool hasMissingMessage = missingMessages.Cast<bool>().Any(miss => miss);
        if (manipulationPassed && firstErrorSource == -1 && !hasMismatchMessage && !hasMissingMessage)
        {
            testOutput.WriteLine($"Raft test {testName} passed.");
            return true;
        }
        testOutput.WriteLine($"Raft test {testName} failed.");
        testOutput.WriteLine(manipulationDebugMsg);
        if (hasMismatchMessage)
        {
            for (int i = 0; i < nodes.Length; i++)
                for (int j = 0; j < nodes.Length; j++)
                    if (connections[i , j].HasMismatchMessage)
                        testOutput.WriteLine($"Mismatch message type on {i} and {j}.");
        }
        if (hasMissingMessage)
        {
            for (int i = 0; i < nodes.Length; i++)
                for (int j = 0; j < nodes.Length; j++)
                    if (missingMessages[i , j])
                        testOutput.WriteLine($"Missing message on {i} and {j} (missing {connections[i , j].ExpectedMessagesChannel.Reader.Count} message).");
        }
        if (hasMismatchMessage || hasMissingMessage)
        {
            (DateTime Time , RaftNode sourceNode , RaftNode targetNode , MessagePackageBase Expected , MessagePackageBase Actual)[] logs =
            [
                ..  from connection in connections.Cast<NetworkConnection>()
                    from log in connection.DebugLogs
                    orderby log.Time
                    select (log.Time , connection.Source , connection.Target , log.Expected , log.Actual) ,
            ];
            (StringBuilder expectOutputBuilder , StringBuilder actualOutputBuilder) = (new StringBuilder() , new StringBuilder());
            foreach ((_ , RaftNode sourceNode , RaftNode targetNode , MessagePackageBase expect , MessagePackageBase actual) in logs)
            {
                expectOutputBuilder.Append($"\t{NetworkConnection.MessagePackageToString(sourceNode , targetNode , expect)}\n");
                actualOutputBuilder.Append($"\t{NetworkConnection.MessagePackageToString(sourceNode , targetNode , actual)}\n");
            }
            testOutput.WriteLine("Global expect logs:");
            testOutput.WriteLine(expectOutputBuilder.ToString());
            testOutput.WriteLine("But get:");
            testOutput.WriteLine(actualOutputBuilder.ToString());

            if (firstErrorSource != -1)
                testOutput.WriteLine("\n\n");
        }
        if (firstErrorSource != -1)
        {
            (DateTime Time , RaftNode sourceNode , RaftNode targetNode , MessagePackageBase Expected , MessagePackageBase Actual)[] logDiffs =
            [
                ..  from connection in connections.Cast<NetworkConnection>()
                    where connection.Source == nodes[firstErrorSource]
                    from log in connection.DebugLogs
                    orderby log.Time
                    select (log.Time , connection.Source , connection.Target , log.Expected , log.Actual) ,
            ];
            (StringBuilder expectOutputBuilder , StringBuilder actualOutputBuilder) = (new StringBuilder() , new StringBuilder());
            foreach ((_ , RaftNode sourceNode , RaftNode targetNode , MessagePackageBase expect , MessagePackageBase actual) in logDiffs)
            {
                expectOutputBuilder.Append($"\t{NetworkConnection.MessagePackageToString(sourceNode , targetNode , expect)}\n");
                actualOutputBuilder.Append($"\t{NetworkConnection.MessagePackageToString(sourceNode , targetNode , actual)}\n");
            }
            testOutput.WriteLine($"Expected (for node {firstErrorSource}):");
            testOutput.WriteLine(expectOutputBuilder.ToString());
            testOutput.WriteLine("But get:");
            testOutput.WriteLine(actualOutputBuilder.ToString());
        }
        return false;
    }
}
