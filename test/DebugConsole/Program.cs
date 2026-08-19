using RaftConsensusOnAspnet.RaftConsensus.Core;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using static DebugConsole.Program.NetworkConnection;


namespace DebugConsole;

internal class Program
{
    private const int MaxTestTime = 10000;
    private const string MaxTestTimeExceedMsg = "Test failed: overtimed.";


    public static async Task Main(string[] args)
    {
        const string SpacingBetweenTests = "\n\n\n\n\n\n";

        Directory.CreateDirectory("debug");
        List<(int testId , string TestName , bool Success)> testCaseStates = [];

        /* Leader election tests. */
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

        /* Log replication tests. */
        testCaseStates.Add((7 , "testOneSimplePut" , await RunTestCase7TestOneSimplePutAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((8 , "testOneSimpleUpdate" , await RunTestCase8TestOneSimpleUpdateAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((9 , "testOneSimpleDelete" , await RunTestCase9TestOneSimpleDeleteAsync()));
        Console.WriteLine(SpacingBetweenTests);
        testCaseStates.Add((10 , "testDeleteNonExistKey" , await RunTestCase10TestDeleteNonExistKeyAsync())); 
        Console.WriteLine(SpacingBetweenTests);


        /* Print out result summary. */
        Console.WriteLine($"Test result summary ({testCaseStates.Count(item => item.Success)}/{testCaseStates.Count} passed):");
        foreach ((int testId , string testName , bool success) in testCaseStates)
            Console.WriteLine($"\tTest {testId:D2} {testName} {(success ? "passed" : "failed")}");
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


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(1000);
            nodes[2].SetElectionTimeoutInterval(1000);
            nodes[3].SetElectionTimeoutInterval(1000);
            nodes[4].SetElectionTimeoutInterval(1000);
            await Task.Delay(1600);
            nodes[4].SetElectionTimeoutInterval(300);
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


        async Task<(bool Success , string DebugMsg)> ManipulateNodes()
        {
            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
            await Task.Delay(1100);
            nodes[4].SetElectionTimeoutInterval(300);
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

    private static async Task<bool> RunTestCase7TestOneSimplePutAsync()
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
        return await CreateAndRunTestCaseAsync(7 , "testOneSimplePut" , nodes , ManipulateNodes , CreateConfig);


        async Task<(bool Success, string DebugMsg)> ManipulateNodes()
        {
            (bool manipulationPassed , StringBuilder debugMsgBuilder)= (true , new StringBuilder());
            Task watchDogTimer = Task.Delay(MaxTestTime);

            await Task.Delay(2000);
            nodes[0].SetElectionTimeoutInterval(1000);
            nodes[1].SetElectionTimeoutInterval(2000);
            nodes[2].SetElectionTimeoutInterval(2000);
            nodes[3].SetElectionTimeoutInterval(2000);
            nodes[4].SetElectionTimeoutInterval(2000);
            nodes[0].SetHeartBeatInterval(1000);

            await Task.Delay(1500);
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] proposeTasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];
            if (Task.WhenAny(watchDogTimer , Task.WhenAll(proposeTasks)) == watchDogTimer)
                return (false , MaxTestTimeExceedMsg);

            /* Verify propose. */
            await Task.Delay(2000);
            if (await proposeTasks[0] is not { Success: true , WrongNode: false , KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair to node 0 failed. Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await proposeTasks[0]).Success , (await proposeTasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await proposeTasks[i] is not { Success: false , WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            string.Format(
                                    "Proposing new key-value pair to node {0} failed. Expect: Success = false, WrongNode = true. but get: Success = {1}, WrongNode = {2}\n" ,
                                    i , (await proposeTasks[i]).Success , (await proposeTasks[i]).WrongNode
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
                            string.Format(
                                    "Get value from node {0} failed. Expect: Success = True, KeyFound = True, Value = 1. but get: Success = {1}, KeyFound = {2}, Value = {3}\n" ,
                                    i , success , keyFound , Equals(value , ExpectValue) ? ExpectValue : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            LogEntry entry = new LogEntry(1 , LogEntryOperation.Put , "test" , 1);
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

    private static async Task<bool> RunTestCase8TestOneSimpleUpdateAsync()
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
        return await CreateAndRunTestCaseAsync(8 , "testOneSimpleUpdate" , nodes , ManipulateNodes , CreateConfig);


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
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];

            await Task.Delay(1000);
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 2) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(2000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false , KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose1Tasks[i] is not { Success: false, WrongNode: true})
                {
                    debugMsgBuilder.Append(
                            string.Format(
                                    "Proposing new key-value pair to node {0} failed. Expect: Success = false, WrongNode = true. but get: Success = {1}, WrongNode = {2}\n" ,
                                    i , (await propose1Tasks[i]).Success , (await propose1Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Check propose 2 (Term 1: Put <test: 2>). */
            if (await propose2Tasks[0] is not { Success: true, WrongNode: false , KeyFound: true })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 2 failed. Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks[0]).Success , (await propose2Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose2Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            string.Format(
                                    "Proposing new key-value pair to node {0} failed. Expect: Success = false, WrongNode = true. but get: Success = {1}, WrongNode = {2}\n" ,
                                    i , (await propose2Tasks[i]).Success , (await propose2Tasks[i]).WrongNode
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
                            string.Format(
                                    "Get value from node {0} failed. Expect: Success = True, KeyFound = True, Value = 1. but get: Success = {1}, KeyFound = {2}, Value = {3}\n" ,
                                    i , success , keyFound , Equals(value , ExpectValue) ? ExpectValue : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            LogEntry entry1 = new LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            LogEntry entry2 = new LogEntry(1 , LogEntryOperation.Put , "test" , 2);
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

    private static async Task<bool> RunTestCase9TestOneSimpleDeleteAsync()
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
        return await CreateAndRunTestCaseAsync(9 , "testOneSimpleDelete" , nodes , ManipulateNodes , CreateConfig);


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
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];

            await Task.Delay(1000);
            Task<(bool Success, bool WrongNode, bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Delete , "test" , null) ,
                nodes[1].ProposeAsync(LogEntryOperation.Delete , "test" , null) ,
                nodes[2].ProposeAsync(LogEntryOperation.Delete , "test" , null) ,
                nodes[3].ProposeAsync(LogEntryOperation.Delete , "test" , null) ,
                nodes[4].ProposeAsync(LogEntryOperation.Delete , "test" , null) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(4000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false , KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. Expect: Success = True, WrongNode = False. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose1Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            string.Format(
                                    "Proposing new key-value (1) pair to node {0} failed. Expect: Success = False, WrongNode = True. but get: Success = {1}, WrongNode = {2}\n" ,
                                    i , (await propose1Tasks[i]).Success , (await propose1Tasks[i]).WrongNode
                                )
                        );
                    manipulationPassed = false;
                }

            /* Check propose 2 (Term 1: Delete <test: 2>). */
            if (await propose2Tasks[0] is not { Success: true, WrongNode: false , KeyFound: true })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. Expect: Success = True, WrongNode = False. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose2Tasks[0]).Success , (await propose2Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }
            for (int i = 1; i < nodes.Length; i++)
                if (await propose2Tasks[i] is not { Success: false, WrongNode: true })
                {
                    debugMsgBuilder.Append(
                            string.Format(
                                    "Proposing new key-value (1) pair to node {0} failed. Expect: Success = False, WrongNode = True. but get: Success = {1}, WrongNode = {2}\n" ,
                                    i , (await propose2Tasks[i]).Success , (await propose2Tasks[i]).WrongNode
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
                            string.Format(
                                    "Get value from node {0} failed. Expect: Success = True, KeyFound = False, Value = null. but get: Success = {1}, KeyFound = {2}, Value = {3}\n" ,
                                    i , success , keyFound , Equals(value , null) ? "null" : 1
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            // heart beat #1
            LogEntry entry1 = new LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 });

            // heart beat #2
            LogEntry entry2 = new LogEntry(1 , LogEntryOperation.Delete , "test" , null);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [         entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true , PreviousLogIndex = 1 , PreviousLogTerm = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 2 });

            // heart beat #3
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1 , entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [         entry2] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 , PreviousLogIndex = 1 , PreviousLogTerm = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [               ] , LeaderId = nodes[0].NodeId , LeaderCommit = 1 , PreviousLogIndex = 2 , PreviousLogTerm = 1 });
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

    private static async Task<bool> RunTestCase10TestDeleteNonExistKeyAsync()
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
        return await CreateAndRunTestCaseAsync(10 , "testDeleteNonExistKey" , nodes , ManipulateNodes , CreateConfig);


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
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] propose1Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[1].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[2].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[3].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
                nodes[4].ProposeAsync(LogEntryOperation.Put , "test" , 1) ,
            ];

            await Task.Delay(1000);
            Task<(bool Success, bool WrongNode , bool? KeyFound)>[] propose2Tasks =
            [
                nodes[0].ProposeAsync(LogEntryOperation.Delete , "test2" , null) ,
                nodes[1].ProposeAsync(LogEntryOperation.Delete , "test2" , null) ,
                nodes[2].ProposeAsync(LogEntryOperation.Delete , "test2" , null) ,
                nodes[3].ProposeAsync(LogEntryOperation.Delete , "test2" , null) ,
                nodes[4].ProposeAsync(LogEntryOperation.Delete , "test2" , null) ,
            ];

            /* Wait for all task to finish. */
            bool propose1Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose1Tasks)) != watchDogTimer;
            bool propose2Finished = Task.WhenAny(watchDogTimer , Task.WhenAll(propose2Tasks)) != watchDogTimer;
            if (!propose1Finished || !propose2Finished)
                return (false , MaxTestTimeExceedMsg);

            /* Check propose 1 (Term 1: Put <test: 1>). */
            await Task.Delay(1000);
            if (await propose1Tasks[0] is not { Success: true, WrongNode: false , KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 0 failed. Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
                                (await propose1Tasks[0]).Success , (await propose1Tasks[0]).WrongNode
                            )
                    );
                manipulationPassed = false;
            }

            /* Check propose 2 (Term 1: Delete <test: 2>). */
            if (await propose2Tasks[0] is not { Success: false , WrongNode: false , KeyFound: false })
            {
                debugMsgBuilder.Append(
                        string.Format(
                                "Proposing new key-value pair (1) to node 2 failed. Expect: Success = true, WrongNode = false. but get: Success = {0}, WrongNode = {1}\n" ,
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
                            string.Format(
                                    "Get value from node {0} failed. Expect: Success = True, KeyFound = True , Value = null. but get: Success = {1}, KeyFound = {2}, Value = {3}\n" ,
                                    i , success , keyFound , Equals(value , 1) ? 1 : "N/A"
                                )
                        );
                    manipulationPassed = false;
                }
            }

            return (manipulationPassed , debugMsgBuilder.ToString());
        }

        async Task<NetworkConnection[,]> CreateConfig(NetworkConnection[,] connections)
        {
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestSendPackage { Term = 1 });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new VoteRequestReceivePackage { Term = 1 , Granted = true });

            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true });

            // heart beat #1
            LogEntry entry1 = new LogEntry(1 , LogEntryOperation.Put , "test" , 1);
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 2].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 3].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesSendPackage { Term = 1 , Entries = [entry1] , LeaderId = nodes[0].NodeId });
            await connections[0 , 1].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });
            await connections[0 , 4].ExpectedMessagesChannel.Writer.WriteAsync(new AppendEntriesReceivePackage { Term = 1 , Success = true , MatchIndex = 1 , MessageDropped = true });

            // heart beat #2
            LogEntry entry2 = new LogEntry(1 , LogEntryOperation.Delete , "test2" , null);
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

    private static async Task<bool> CreateAndRunTestCaseAsync(
        int testId , string testName , RaftNode[] nodes ,
        Func<Task<(bool Success , string DebugMsg)>> nodeManipulation ,  Func<NetworkConnection[,] , Task<NetworkConnection[,]>> configs)
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

        /* Start raft nodes. */
        Task[] raftStartTasks = new Task[nodes.Length];
        for (int i = 0; i < nodes.Length; i++)
            raftStartTasks[i] = nodes[i].StartAsync();
        (bool manipulationPassed , string manipulationDebugMsg) = await nodeManipulation.Invoke();
        Task watchDogInterrupt = Task.Delay(MaxTestTime);

        /* Wait to finalize the test case. */
        List<Task> waitForExpectMsgChannelReaderCloses = new List<Task>(connections.Length);
        foreach (NetworkConnection connection in connections.Cast<NetworkConnection>())
        {
            connection.ExpectedMessagesChannel.Writer.Complete();
            waitForExpectMsgChannelReaderCloses.Add(connection.ExpectedMessagesChannel.Reader.Completion);
        }
        Task waitForAllExpectMsgChannelClose = Task.WhenAll(waitForExpectMsgChannelReaderCloses);
        Task finishedTask = await Task.WhenAny(waitForAllExpectMsgChannelClose , watchDogInterrupt);
        if (finishedTask == watchDogInterrupt)
        {
            Console.WriteLine(MaxTestTimeExceedMsg);
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
        if (manipulationPassed && firstErrorSource == -1 && !hasMismatchMessage && !hasMissingMessage)
        {
            Console.WriteLine($"Raft test {testId} {testName} passed.");
            return true;
        }
        Console.WriteLine($"Raft test {testId} {testName} failed.");
        Console.WriteLine(manipulationDebugMsg);
        if (hasMismatchMessage)
        {
            for (int i = 0; i < nodes.Length; i++)
                for (int j = 0; j < nodes.Length; j++)
                    if (connections[i , j].HasMismatchMessage)
                        Console.WriteLine($"Mismatch message type on {i} and {j}.");
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
                    identical &= Entries[i].MemberWiseEqualityCheck(other.Entries[i]);

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
