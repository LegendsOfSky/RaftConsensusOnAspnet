using RaftConsensusOnAspnet.RaftConsensus.Core;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using static DebugConsole.Program.NetworkConnection;


namespace DebugConsole;

internal class Program
{
    public static async Task Main(string[] args)
    {
        const string SpacingBetweenTests = "\n\n\n\n\n\n";
        Directory.CreateDirectory("debug");
        List<(int testId , string TestName , bool Success)> testCaseStates = [];
        testCaseStates.Add((1 , "TestOneCandidateOneRoundElection" , await RunTestCase1TestOneCandidateOneRoundElectionAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((2 , "TestOneCandidateStartTwoElection" , await RunTestCase2TestOneCandidateStartTwoElectionAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((3 , "TestTwoCandidateForElection" , await RunTestCase3TestTwoCandidateForElectionAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((4 , "TestSplitVote" , await RunTestCase4TestSplitVoteAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((5 , "testAllForElection" , await RunTestCase5TestAllForElectionAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((6 , "testLeaderRevertToFollower" , await RunTestCase6TestLeaderRevertToFollowerAsync()));

        Console.WriteLine(SpacingBetweenTests);
        Console.WriteLine("Test result summary:");
        foreach ((int testId, string testName, bool success) in testCaseStates)
            Console.WriteLine($"\tTest {testId} {testName} {(success ? "passed" : "failed")}");
    }

    private static async Task<bool> RunTestCase1TestOneCandidateOneRoundElectionAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(1 , "testOneCandidateOneRoundElection" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
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

    private static async Task<bool> RunTestCase2TestOneCandidateStartTwoElectionAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(2 , "testOneCandidateStartTwoElection" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(3000);
            nodes[4].SetElectionTimeoutInterval(3000);
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

    private static async Task<bool> RunTestCase3TestTwoCandidateForElectionAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(3 , "testTwoCandidateForElection" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(2000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(4000);
            nodes[4].SetElectionTimeoutInterval(4000);
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

    private static async Task<bool> RunTestCase4TestSplitVoteAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(4 , "testSplitVote" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(2000);
            nodes[1].SetElectionTimeoutInterval(3000);
            nodes[2].SetElectionTimeoutInterval(3000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(3000);
            await Task.Delay(2100);
            nodes[3].SetElectionTimeoutInterval(300);
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            // T = 2000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });

            // T = 2000
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 600 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 600 });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 600 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 600 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 , Delay = 600 });

            // T = 2000
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true , MessageDropped = true });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            // T = 2400
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            // T = 2400
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[3].NodeId });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[3].NodeId });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[3].NodeId });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 2 , Entries = [] , LeaderId = nodes[3].NodeId });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 2 , Success = true });

            // T = 2600
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = false });

            return connections;
        }
    }

    private static async Task<bool> RunTestCase5TestAllForElectionAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(5 , "testAllForElection" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(1000);
            nodes[2].SetElectionTimeoutInterval(1000);
            nodes[3].SetElectionTimeoutInterval(1000);
            nodes[4].SetElectionTimeoutInterval(1000);
            await Task.Delay(1600);
            nodes[4].SetElectionTimeoutInterval(300);
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
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});

            // T = 1500
            await connections[1 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[1 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[1 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[1 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});

            // T = 1500
            await connections[2 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[2 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[2 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[2 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});

            // T = 1500
            await connections[3 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[3 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[3 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[3 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});

            // T = 1500
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = false , Delay = 500});

            // T = 1600
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 2 });

            // T = 1600
            await connections[4 , 0].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });
            await connections[4 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 2 , Granted = true });

            // T = 1600
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

    private static async Task<bool> RunTestCase6TestLeaderRevertToFollowerAsync()
    {
        RaftNode.S_nodeIdToDebugPos = new Dictionary<Guid , int>();
        RaftNode[] nodes =
        [
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
            new RaftNode(10000 , 10000 , 5) ,
        ];
        return await CreateAndRunTestCaseAsync(6 , "testOneCandidateOneRoundElection" , nodes , ManipulateNodes , CreateConfig);


        async Task ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
            await Task.Delay(1100);
            nodes[4].SetElectionTimeoutInterval(300);
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

    private static async Task<bool> CreateAndRunTestCaseAsync(
        int testId , string testName , RaftNode[] nodes ,
        Func<Task> nodeManipulation ,  Func<NetworkConnection[,] , Task<NetworkConnection[,]>> configs)
    {
        Trace.Listeners.Clear();
        Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
        Trace.Listeners.Add(new TextWriterTraceListener($"debug/{testId:D2}_{testName}.log"));
        Debug.AutoFlush = true;
        Console.WriteLine($"Begin test {testId} ({testName})");

        /* Prepare test enviroment. */
        NetworkConnection[,] connections = new NetworkConnection[nodes.Length , nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
        {
            for (int j = 0; j < nodes.Length; j++)
            {
                connections[i , j] = new NetworkConnection(nodes[i] , nodes[j]);

                if (i == j)
                    continue;

                nodes[i].SendVoteRequestToOtherNodes += connections[i , j].HandleVoteRequestAsync;
                nodes[i].AppendEntriesToOtherNodes += connections[i , j].HandleAppendEntriesAsync;
            }
        }
        await configs.Invoke(connections);  // encode the correct raft node behavior
        Task watchDogInterrupt = Task.Delay(10000);

        /* Start raft nodes. */
        Task[] raftStartTasks = new Task[nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
            raftStartTasks[i] = nodes[i].StartAsync();
        await nodeManipulation.Invoke();

        /* Wait to finalize the test case. */
        List<Task> waitForExpectMsgChannelReaderCloses = new List<Task>(connections.Length);
        foreach (NetworkConnection connection in connections.Cast<NetworkConnection>())
        {
            connection.ExpectedMessagesChannel.Writer.Complete();
            waitForExpectMsgChannelReaderCloses.Add(connection.ExpectedMessagesChannel.Reader.Completion);
        }
        Task finishedTask = await Task.WhenAny(Task.WhenAll(waitForExpectMsgChannelReaderCloses) , watchDogInterrupt);
        if (finishedTask == watchDogInterrupt)
        {
            Console.WriteLine("Test failed: overtimed.");
            return false;
        }
        Task[] stopAllRaftNodes = new Task[nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
            stopAllRaftNodes[i] = nodes[i].StopAsync();
        await Task.WhenAll(stopAllRaftNodes);
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
        if (firstErrorSource == -1 && !hasMismatchMessage && !hasMissingMessage)
        {
            Console.WriteLine($"Raft test {testId} {testName} passed.");
            return true;
        }
        Console.WriteLine($"Raft test {testId} {testName} failed.");
        if (hasMismatchMessage)
        {
            for (int i = 0; i < nodes.Length; i++)
                for (int j = 0; j < nodes.Length; j++)
                    if (connections[i , j].HasMismatchMessage)
                        Console.WriteLine($"Mismatch message on {i} and {j}.");
        }
        if (hasMissingMessage)
        {
            for (int i = 0; i < nodes.Length; i++)
                for (int j = 0; j < nodes.Length; j++)
                    if (missingMessages[i , j])
                        Console.WriteLine($"Missing message on {i} and {j} (missing {connections[i , j].ExpectedMessagesChannel.Reader.Count} message).");
        }
        if (hasMismatchMessage || hasMissingMessage)
        {
            (DateTime Time , int sourceIndex , int targetIndex , MessagePackageBase Expected , MessagePackageBase Actual)[] logs =
            [
                .. from connection in connections.Cast<NetworkConnection>()
                   let sourceIndex = nodes.IndexOf(connection.Source)
                   let targetIndex = nodes.IndexOf(connection.Target)
                   from log in connection.DebugLogs
                   orderby log.Time
                   select (log.Time , sourceIndex , targetIndex , log.Expected , log.Actual) ,
            ];
            Console.WriteLine("Global expect logs:");
            foreach ((_ , int sourceIndex , int targetIndex , MessagePackageBase expect , _) in logs)
                Console.WriteLine($"\t{MessagePackageToString(sourceIndex , targetIndex , expect)}");
            Console.WriteLine("But get:");
            foreach ((_ , int sourceIndex , int targetIndex , _ , MessagePackageBase actual) in logs)
                Console.WriteLine($"\t{MessagePackageToString(sourceIndex , targetIndex , actual)}");

            if (firstErrorSource != -1)
                Console.WriteLine("\n\n");
        }
        if (firstErrorSource != -1)
        {
            (DateTime Time , int sourceIndex , int targetIndex , MessagePackageBase Expected , MessagePackageBase Actual)[] logDiffs =
            [
                .. from connection in connections.Cast<NetworkConnection>()
                   where connection.Source == nodes[firstErrorSource]
                   let sourceIndex = nodes.IndexOf(connection.Source)
                   let targetIndex = nodes.IndexOf(connection.Target)
                   from log in connection.DebugLogs
                   orderby log.Time
                   select (log.Time , sourceIndex , targetIndex , log.Expected , log.Actual) ,
            ];
            Console.WriteLine($"Expected (for node {firstErrorSource}):");
            foreach ((_ , int sourceIndex , int targetIndex , MessagePackageBase expect , _) in logDiffs)
                Console.WriteLine($"\t{MessagePackageToString(sourceIndex , targetIndex , expect)}");
            Console.WriteLine("But get:");
            foreach ((_ , int sourceIndex , int targetIndex , _ , MessagePackageBase actual) in logDiffs)
                Console.WriteLine($"\t{MessagePackageToString(sourceIndex , targetIndex , actual)}");
        }
        return false;
    }

    private static string MessagePackageToString(int source , int target , MessagePackageBase message)
    {
        switch (message)
        {
            case VoteRequestSendPackage voteSend:
                return voteSend.MessageDropped
                    ? $"node {source}: dropped RequestVote to {target}"
                    : string.Format(
                            "node {0} <- {1}: RequestVote -- term: {2}, candidateId: {3}, lastLogIdx: {4}, lastLogTerm: {5}" ,
                            target , source , voteSend.Term , source , voteSend.LastLogIndex , voteSend.LastLogTerm
                        );

            case VoteRequestReceivePackage voteRecv:
                return voteRecv.MessageDropped
                    ? $"node {target}: dropped RequestVoteResponse to {source}"
                    : $"node {target} -> {source}: {(voteRecv.Granted ? "granted" : "reject")}, term: {voteRecv.Term}";

            case AppendEntriesSendPackage appendSend:
                return appendSend.MessageDropped
                    ? $"node {source}: dropped AppendEntries to {target}"
                    : string.Format(
                            "node {0} <- {1}: AppendEntries -- term: {2}, leaderId: {3}, prevLogIdx: {4}, prevLogTerm: {5}, entries: [{6}], leaderCommit: {7}" ,
                            source , target ,
                            appendSend.Term , RaftNode.S_nodeIdToDebugPos[appendSend.LeaderId] , appendSend.PreviousLogIndex , appendSend.PreviousLogTerm ,
                            new StringBuilder().AppendJoin(' ' , appendSend.Entries.Select(entry => entry.ToString())) ,
                            appendSend.LeaderCommit
                        );

            case AppendEntriesReceivePackage appendRecv:
                return appendRecv.MessageDropped
                    ? $"node {target}: dropped AppendEntriesReponse to {source}"
                    : $"node {target} -> {source}: {(appendRecv.Success ? "success" : "failed")}, term: {appendRecv.Term}, matchIdx: {appendRecv.MatchIndex}";

            default:
                return "";
        }
    }


    public class NetworkConnection
    {
        public RaftNode Source;
        public RaftNode Target;
        public bool HasMismatchMessage;
        public Channel<MessagePackageBase> ExpectedMessagesChannel = Channel.CreateUnbounded<MessagePackageBase>();
        public List<(MessagePackageBase Expected , MessagePackageBase Actual , DateTime Time)> DebugLogs = [];


        public NetworkConnection(RaftNode source , RaftNode target)
        {
            (Source , Target) = (source , target);
        }


        public async Task<bool> HandleVoteRequestAsync(Guid requesterId , int commitIndex , int previousLogTerm)
        {
            VoteRequestArgs args = new VoteRequestArgs
            {
                RequesterId = Source.NodeId ,
                ReceiverId = Target.NodeId ,
                RequesterTerm =  Source.CurrentTerm ,

                RequesterLastLogIndex = commitIndex ,
                RequesterLastLogTerm = previousLogTerm ,
            };
            VoteRequestSendPackage actualSendPackage = new VoteRequestSendPackage
            {
                MessageDropped = true ,
                Term = args.RequesterTerm ,

                LastLogIndex = args.RequesterLastLogIndex ,
                LastLogTerm = args.RequesterLastLogTerm ,
            };

            MessagePackageBase expectSendPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectSendPackageBase is not VoteRequestSendPackage expectSendPackage)
            {
                DebugLogs.Add((expectSendPackageBase , actualSendPackage , DateTime.Now));
                HasMismatchMessage = true;
                return false;
            }

            (actualSendPackage.Delay , actualSendPackage.MessageDropped) = (expectSendPackage.Delay , expectSendPackage.MessageDropped);
            DebugLogs.Add((expectSendPackage , actualSendPackage , DateTime.Now));
            await Task.Delay(expectSendPackage.Delay);
            Console.WriteLine(
                    MessagePackageToString(RaftNode.S_nodeIdToDebugPos[args.RequesterId] , RaftNode.S_nodeIdToDebugPos[args.ReceiverId] , expectSendPackage)
                );
            if (expectSendPackage.MessageDropped)
                return true;

            VoteRequestReply reply = Target.HandleVoteRequest(args);  // forward request to actual raft node
            VoteRequestReceivePackage actualRecvPackage = new VoteRequestReceivePackage
            {
                MessageDropped = true ,
                Term = reply.ReplierTerm ,

                Granted = reply.VoteGranted ,
            };

            MessagePackageBase expectRecvPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectRecvPackageBase is not VoteRequestReceivePackage expectRecvPackage)
            {
                DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
                HasMismatchMessage = true;
                return false;
            }

            await Task.Delay(expectRecvPackage.Delay);
            (actualRecvPackage.Delay , actualRecvPackage.MessageDropped) = (expectRecvPackage.Delay , expectRecvPackage.MessageDropped);
            DebugLogs.Add((expectRecvPackage , actualRecvPackage , DateTime.Now));
            Console.WriteLine( 
                    MessagePackageToString(RaftNode.S_nodeIdToDebugPos[reply.ReceiverId] , RaftNode.S_nodeIdToDebugPos[reply.ReplierId] , expectRecvPackage)
                );
            if (!expectRecvPackage.MessageDropped)
                await Source.VoteRequestReplyChannel.Writer.WriteAsync(reply);
            return true;
        }

        public async Task<bool> HandleAppendEntriesAsync(
            Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndecies)
        {
            int nextIndex = nextIndecies.TryGetValue(Target.NodeId , out int i) ? i : nextIndecies[Guid.Empty];
            AppendEntriesArgs args = new AppendEntriesArgs
            {
                RequesterId = Source.NodeId ,
                ReceiverId = Target.NodeId ,
                RequesterTerm = Source.CurrentTerm ,

                LeaderCommit = commitIndex ,
                PreviousLogIndex = nextIndex - 1 ,
                PreviousLogTerm = logEntries[nextIndex - 1].Term ,
                Entries = [.. logEntries.Skip(nextIndex)] ,
            };
            AppendEntriesSendPackage actualSendPackage = new AppendEntriesSendPackage()
            {
                MessageDropped = true ,
                Term = args.RequesterTerm ,

                LeaderId = args.RequesterId ,
                LeaderCommit = args.LeaderCommit ,
                PreviousLogIndex = args.PreviousLogIndex ,
                PreviousLogTerm = args.PreviousLogTerm ,
                Entries = args.Entries ,
            };

            MessagePackageBase expectSendPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectSendPackageBase is not AppendEntriesSendPackage expectSendPackage)
            {
                DebugLogs.Add((expectSendPackageBase , actualSendPackage , DateTime.Now));
                HasMismatchMessage = true;
                return false;
            }

            (actualSendPackage.Delay , actualSendPackage.MessageDropped) = (expectSendPackage.Delay , expectSendPackage.MessageDropped);
            DebugLogs.Add((expectSendPackage , actualSendPackage , DateTime.Now));
            await Task.Delay(expectSendPackage.Delay);
            Console.WriteLine(
                    MessagePackageToString(RaftNode.S_nodeIdToDebugPos[args.RequesterId] , RaftNode.S_nodeIdToDebugPos[args.ReceiverId] , expectSendPackage)
                );
            if (expectSendPackage.MessageDropped)
                return true;

            AppendEntriesReply reply = Target.HandleAppendEntries(args);  // forward request to actual raft node
            AppendEntriesReceivePackage actualRecvPackage = new AppendEntriesReceivePackage
            {
                MessageDropped = true ,
                Term = reply.ReplierTerm ,

                Success = reply.AppendSuccess ,
                MatchIndex = reply.MatchIndex ,
            };

            MessagePackageBase expectRecvPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectRecvPackageBase is not AppendEntriesReceivePackage expectRecvPackage)
            {
                DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
                HasMismatchMessage = true;
                return false;
            }

            await Task.Delay(expectRecvPackageBase.Delay);
            (actualRecvPackage.Delay , actualRecvPackage.MessageDropped) = (expectRecvPackage.Delay , expectRecvPackage.MessageDropped);
            DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
            Console.WriteLine(
                    MessagePackageToString(RaftNode.S_nodeIdToDebugPos[reply.ReceiverId] , RaftNode.S_nodeIdToDebugPos[reply.ReplierId] , expectRecvPackageBase)
                );
            if (!expectRecvPackageBase.MessageDropped)
                await Source.AppendEntriesReplyChannel.Writer.WriteAsync(reply);
            return true;
        }


        public abstract record MessagePackageBase
        {
            public bool MessageDropped;
            public int Delay;
            public int Term;
        }

        public record VoteRequestSendPackage : MessagePackageBase
        {
            public int LastLogIndex ;
            public int LastLogTerm;
        }

        public record VoteRequestReceivePackage : MessagePackageBase
        {
            public bool Granted;
        }

        public record AppendEntriesSendPackage : MessagePackageBase
        {
            public Guid LeaderId;
            public int LeaderCommit;
            public int PreviousLogIndex;
            public int PreviousLogTerm;
            public IReadOnlyList<LogEntry> Entries;


            /// <inheritdoc />
            public virtual bool Equals(AppendEntriesSendPackage? other)
            {
                if (other is null || this.Entries.Count != other.Entries.Count)
                    return false;

                bool identical = true;
                identical &= LeaderId         == other.LeaderId;
                identical &= LeaderCommit     == other.LeaderCommit;
                identical &= PreviousLogIndex == other.PreviousLogIndex;
                identical &= PreviousLogTerm  == other.PreviousLogTerm;

                for (int i = 0; i < Entries.Count; i++)
                    identical &= this.Entries[i] == other.Entries[i];

                return identical;
            }
        }

        public record AppendEntriesReceivePackage : MessagePackageBase
        {
            public bool Success;
            public int MatchIndex;
        }
    }
}
