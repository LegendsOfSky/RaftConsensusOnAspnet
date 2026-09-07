using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class RaftNode
{
    public int CurrentTerm
    {
        get;
        private set
        {
            WriteCurrentTermToDb(value);
            field = ReadCurrentTermFromDb();
        }
    }
    public int LastAppliedLogEntryIndex => logEntries.LastAppliedIndex;
    public NodeRole Role { get; private set; }
    public Guid? LeaderId { get; private set; }
    public IReadOnlyList<LogEntry> LogEntries => logEntries;
    private (Guid VoteFor , int TermOfVote) VoteInfo
    {
        get;
        set
        {
            WriteVoteInfoToDb(value.VoteFor , value.TermOfVote);
            field = ReadVoteInfoFromDb();
        }
    }

    public const string DataPath = "data";
    private const string TableName = "RaftNodeState";

    /// <summary> Debug purpose, messing this up has no any effect on Raft behaviour. (except printing invalid debug logs) </summary>
    public readonly Dictionary<Guid , int> NodeIdToDebugPos;

    public readonly Guid NodeId;
    public readonly Channel<AppendEntriesReply> AppendEntriesReplyChannel;
    public readonly Channel<VoteRequestReply> VoteRequestReplyChannel;
    public Func<Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>? AppendEntriesToOtherNodes;
    public Func<Guid , int , int , Task>? SendVoteRequestToOtherNodes;
    private readonly string dbFilePath;
    private readonly LogEntryList logEntries;
    private int nodeCount;
    private int electionTimeoutInterval;
    private int heartBeatInterval;
    private int commitIndex;
    private Dictionary<Guid , int> nextIndexes;
    private Dictionary<Guid , int> matchIndexes;
    private TaskCompletionSource? stopRaftTcs;
    private TaskCompletionSource changeElectionIntervalTcs;
    private TaskCompletionSource changeHeartBeatIntervalTcs;
    private TaskCompletionSource changeNodeCountTcs;
    private TaskCompletionSource revertToFollowerTcs;
    private TaskCompletionSource receiveHeartBeatTcs;
    private TaskCompletionSource commitNewEntryTcs;


    public RaftNode(int electionTimeOutIntervalIn , int heartBeatIntervalIn , int nodeCountIn , bool removeExistData = false)
        : this(Guid.NewGuid() , electionTimeOutIntervalIn , heartBeatIntervalIn , nodeCountIn , removeExistData) { }

    public RaftNode(Guid guid , int electionTimeOutIntervalIn , int heartBeatIntervalIn , int nodeCountIn ,
                    bool removeExistData = false , Dictionary<Guid , int>? nodeIdToDebugPos = null)
    {
        if (!Directory.Exists(DataPath))
            Directory.CreateDirectory(DataPath);

        NodeId = guid;

        /* Initialize database pragma and schema */
        dbFilePath = $"{DataPath}/{NodeId}.db";
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand createTable = connection.CreateCommand();
        createTable.CommandText = $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                Key   TEXT PRIMARY KEY ,
                Value TEXT
            );
            """;
        createTable.ExecuteNonQuery();

        /* Basic raft fields */
        Role = NodeRole.Follower;
        nodeCount = nodeCountIn;
        CurrentTerm = 0;
        commitIndex = 0;
        (nextIndexes , matchIndexes) = ([] , []);
        VoteInfo = (Guid.Empty , 0);
        (electionTimeoutInterval , heartBeatInterval) = (electionTimeOutIntervalIn , heartBeatIntervalIn);
        logEntries = new LogEntryList(dbFilePath , removeExistData);

        /* For notifications and messaging */
        changeElectionIntervalTcs  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeNodeCountTcs         = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        revertToFollowerTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiveHeartBeatTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitNewEntryTcs          = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        VoteRequestReplyChannel   = Channel.CreateUnbounded<VoteRequestReply>();
        AppendEntriesReplyChannel = Channel.CreateUnbounded<AppendEntriesReply>();

        NodeIdToDebugPos = nodeIdToDebugPos ?? [];
        NodeIdToDebugPos.Add(NodeId , NodeIdToDebugPos.Count);  // only for debug purpose
    }


    #region Major API
    public async Task StartAsync()
    {
        stopRaftTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        logEntries.Add(new LogEntry { Term = 0 });
        Task logEntriesSync = logEntries.StartSynchronizingStateMachineValueAsync();
        while (true)
        {
            if (stopRaftTcs.Task.IsCompleted)
                break;

            switch (Role)
            {
                case NodeRole.Follower:  await RunAsFollowerAsync();  break;
                case NodeRole.Candidate: await RunAsCandidateAsync(); break;
                case NodeRole.Leader:    await RunAsLeaderAsync();    break;
                default: throw new UnreachableException();
            }
        }

        logEntries.StopSynchronizingStateMachineValue();
        await logEntriesSync;
    }

    public void Stop() => stopRaftTcs?.TrySetResult();

    public (bool Success , bool KeyFound , object? Value) GetValue(string key)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"GetValue",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        for (int i = commitIndex; i >= 0; i--)
            if (logEntries[i].Key == key)
                switch (logEntries[i].Operation)
                {
                    case LogEntryOperation.Put:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Value founded ({key}: {logEntries[i].GetValue()}).");
                        return (true , true , logEntries[i].GetValue());
                    case LogEntryOperation.Delete:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: This value (key = {key}) has been deleted.");
                        return (true , false , null);
                }

        return (false , false , null);
    }

    /// <returns>
    ///     A tuple indicates:
    ///     <list type="table">
    ///         <item> <term> Success </term>
    ///             <description> The proposal has finished with no error. </description>
    ///         </item>
    ///         <item> <term> WrongNode </term>
    ///             <description> The node is not leader. </description>
    ///         </item>
    ///         <item> <term> KeyFound </term>
    ///             <description> The key exist before this propose. Null if there is nothing to propose or this node is not raft leader. </description>
    ///         </item>
    ///     </list>
    /// </returns>>
    public async Task<(bool Success , bool WrongNode , bool? KeyFound)> ProposeAsync(LogEntryOperation operation , string? key , object? value)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"Propose",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        /* Ignore new propose when this node is not leader. */
        if (LeaderId != NodeId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Cannot handle new propose because this node is not raft leader.");
            return (false , true , null);
        }

        if (operation == LogEntryOperation.None || key is null)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Nothing to propose.");
            return (true , false , null);
        }

        if (operation == LogEntryOperation.Delete && !CheckKeyExist(key , logEntries.Count))
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: No such key to delete.");
            return (false , false , false);
        }

        LogEntry entry = new Int32LogEntry(CurrentTerm , operation , key , (int?)value);  // FIXME use hard code casting instead of temporary cast
        logEntries.Add(entry);
        int thisLogIndex = logEntries.Index().First(kvp => kvp.Item.Equals(entry)).Index;
        while (true)
        {
            Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
            Task waitForNewEntriesCommitedSignal = commitNewEntryTcs.Task;
            Task finishedTask = await Task.WhenAny(
                    waitForRevertToFollowerSignal ,
                    waitForNewEntriesCommitedSignal
                );
            if (finishedTask == waitForRevertToFollowerSignal)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Cannot handle new propose because this node is not raft leader.");
                return (false , true , null);
            }
            if (finishedTask == waitForNewEntriesCommitedSignal && commitIndex >= thisLogIndex)
                switch (operation)
                {
                    case LogEntryOperation.Put:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Propose {key} success (put).");
                        return (true , false , CheckKeyExist(key , thisLogIndex));

                    case LogEntryOperation.Delete:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Propose {key} success (delete).");
                        return (true , false , true);

                    default: throw new UnreachableException();
                }
        }
    }

    public void SetElectionTimeoutInterval(int electionTimeoutIntervalIn)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetElectionTimeout",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN (interval = {electionTimeoutIntervalIn})");

        electionTimeoutInterval = electionTimeoutIntervalIn;
        changeElectionIntervalTcs.TrySetResult();
        changeElectionIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void SetHeartBeatInterval(int heartBeatIntervalIn)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetHeartBeatInterval",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN (interval = {heartBeatIntervalIn})");

        heartBeatInterval = heartBeatIntervalIn;
        changeHeartBeatIntervalTcs.TrySetResult();
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void UpdateRaftClusterNodeCount(int nodeCountIn)
    {
        nodeCount = nodeCountIn;
        changeNodeCountTcs.TrySetResult();
        changeNodeCountTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    #endregion

    public VoteRequestReply HandleVoteRequest(VoteRequestArgs args)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleVoteRequest",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Handle vote request from node {NodeIdToDebugPos[args.RequesterId]}");

        VoteRequestReply reply = new VoteRequestReply()
        {
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,
            TermOfRequest = args.RequesterTerm ,
            VoteGranted = false ,
        };

        if (args.ReceiverId != NodeId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        /* reject when requester has lower term */
        if (args.RequesterTerm < CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Requester has lower term ({args.RequesterTerm}), vote request rejected.");
            return reply;
        }

        if (args.RequesterTerm > CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Requester has higher term ({args.RequesterTerm}).");
            CurrentTerm = args.RequesterTerm;
            reply.ReplierTerm = args.RequesterTerm;

            switch (Role)
            {
                case NodeRole.Follower:
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Heart beat signal received.");
                    receiveHeartBeatTcs.TrySetResult();
                    receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                case NodeRole.Leader:
                    LeaderId = null;
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Higher term found, revert to follower.");
                    revertToFollowerTcs.TrySetResult();
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                case NodeRole.Candidate:
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Higher term found, revert to follower.");
                    revertToFollowerTcs.TrySetResult();
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                default:
                    throw new UnreachableException();
            }
        }

        /* reject when this node has vote for others */
        if (VoteInfo.TermOfVote == CurrentTerm && VoteInfo.VoteFor != Guid.Empty && VoteInfo.VoteFor != args.RequesterId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: This node has voted for other node ({NodeIdToDebugPos[VoteInfo.VoteFor]}), vote request rejected.");
            return reply;
        }

        if (commitIndex <= args.RequesterLastLogIndex && logEntries[commitIndex].Term <= args.RequesterLastLogTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Vote granted because log of requester is as update as this node.");
            VoteInfo = (args.RequesterId , CurrentTerm);
            return reply with { VoteGranted = true };
        }

        Debug.WriteLine(
                "{0} {1}: By default, reject vote request (thisCommitIndex {2}, otherCommitIndex {3}, thisLastTerm {4}, otherLastTerm {5})." ,
                DateTime.Now.TimeOfDay , loggingPrefix ,
                commitIndex , args.RequesterLastLogIndex , logEntries[commitIndex].Term , args.RequesterLastLogTerm
            );
        return reply;
    }

    public AppendEntriesReply HandleAppendEntries(AppendEntriesArgs args)
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleAppendEntries",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Handle append entries request from node {NodeIdToDebugPos[args.RequesterId]}");

        AppendEntriesReply reply = new AppendEntriesReply
        {
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,

            AppendSuccess = false ,
            MatchIndex = commitIndex ,
        };

        if (args.ReceiverId != NodeId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        if (args.RequesterTerm < CurrentTerm)
        {
            Debug.WriteLine(
                    $"{DateTime.Now.TimeOfDay} {loggingPrefix}: Append entries request REJECTED because requester has an outdated term (Impl Ref #1)."
                );
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Update leader to node {NodeIdToDebugPos[args.RequesterId]}");
        LeaderId = args.RequesterId;

        switch (Role)
        {
            case NodeRole.Follower:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Heart beat signal received");
                receiveHeartBeatTcs.TrySetResult();
                receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Candidate:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Revert to follower (Candidate -> Follower).");
                revertToFollowerTcs.TrySetResult();
                revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Leader:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: TODO (Split leader) finish this state (Code navigation key: lm2leockDs3uGiHJ).");
                break;

            default:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Error, incorrect state (Code navigation key: uKP8WWdVLb2ZlWqN).");
                break;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Updating terms to {args.RequesterTerm}.");
        CurrentTerm = args.RequesterTerm;
        reply.ReplierTerm = CurrentTerm;
        loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleAppendEntries",-20}>";

        if (logEntries.Count <= args.PreviousLogIndex)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Append entries FAILED because some logs are missing prior new entries (Impl Ref #2).");
            return reply;
        }

        if (logEntries[args.PreviousLogIndex].Term != args.PreviousLogTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Remove conflicting logs and subsequent logs (Impl Ref #3).");
            logEntries.RemoveRange(args.PreviousLogIndex , logEntries.Count - args.PreviousLogIndex);
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Append new entries (Impl Ref #4).");
        if (!logEntries.TryEraseAndAppendEntriesAt(args.Entries , args.PreviousLogIndex + 1))
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Append new entries failed.");
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Update commit index (Impl Ref #5).");
        if (args.LeaderCommit > commitIndex)
            commitIndex = Math.Min(logEntries.Count - 1 , args.LeaderCommit);
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Commit index has been set to {commitIndex}.");

        reply.AppendSuccess = true;
        reply.MatchIndex = args.PreviousLogIndex + args.Entries.Count;
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Append entries success (matchIndex = {reply.MatchIndex}).");
        return reply;
    }

    private bool CheckKeyExist(string key , int checkBeforeIndex)
    {
        while (--checkBeforeIndex >= 0)
            if (logEntries[checkBeforeIndex].Key == key)
                switch (logEntries[checkBeforeIndex].Operation)
                {
                    case LogEntryOperation.Put:    return true;
                    case LogEntryOperation.Delete: return false;
                }
        return false;
    }

    private void InitializeLeaderRequiredField()
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"initLeaderReqField",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        nextIndexes = new Dictionary<Guid , int>(nodeCount) { [Guid.Empty] = logEntries.Count };
        matchIndexes = new Dictionary<Guid , int>(nodeCount);
    }

    private int ReadCurrentTermFromDb()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand getCurrentTerm = new SqliteCommand(
                $"""
                SELECT Value
                FROM {TableName}
                WHERE Key = 'CurrentTerm';
                """ , connection
            );
        using SqliteDataReader reader = getCurrentTerm.ExecuteReader();
        return reader.Read() ? int.Parse(reader.GetString(0)) : 0;
    }

    private (Guid VoteFor , int TermOfVote) ReadVoteInfoFromDb()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand getVoteInfo = new SqliteCommand(
                $"""
                SELECT Key , Value
                FROM {TableName}
                WHERE Key = 'VoteFor' OR Key = 'TermOfVote';
                """ , connection
            );
        using SqliteDataReader reader = getVoteInfo.ExecuteReader();
        (Guid voteFor , int termOfVote) = (Guid.Empty , -1);
        while (reader.Read())
        {
            string key = reader.GetString(0);
            if (key == "VoteFor")
                voteFor = reader.GetGuid(1);
            else if (key == "TermOfVote")
                termOfVote = int.Parse(reader.GetString(1));
        }
        return voteFor == Guid.Empty || termOfVote == -1
            ? (Guid.Empty , -1)
            : (voteFor , termOfVote);
    }

    private async Task RunAsCandidateAsync()
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        CurrentTerm++;
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Advancing to term {CurrentTerm}.");
        loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Sending vote request to other nodes.");
        VoteInfo = (NodeId , CurrentTerm);
        if (SendVoteRequestToOtherNodes is null)
            throw new ArgumentNullException(nameof(SendVoteRequestToOtherNodes));
        Array.ForEach(
                SendVoteRequestToOtherNodes.GetInvocationList() ,
                requestFunction => ((Func<Guid , int , int , Task>)requestFunction).Invoke(NodeId , commitIndex , logEntries[commitIndex].Term)
            );

        int voteGranted = 1 , voteReceived = 1;
        while (true)
        {
            Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");

            while (true)
            {
                Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
                Task waitForElectionIntervalChange = changeElectionIntervalTcs.Task;
                Task waitForNodeCountChange = changeNodeCountTcs.Task;
                Task waitForNewVoteRequestReply = VoteRequestReplyChannel.Reader.WaitToReadAsync().AsTask();
                Task completedTask = await Task.WhenAny(
                        waitForElectionTimerEnd ,
                        waitForRevertToFollowerSignal ,
                        waitForElectionIntervalChange ,
                        waitForNodeCountChange ,
                        waitForNewVoteRequestReply
                    );
                if (completedTask == waitForElectionTimerEnd)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Election timeout, restart election.");
                    return;
                }
                if (completedTask == waitForRevertToFollowerSignal)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Revert to follower.");
                    Role = NodeRole.Follower;
                    return;
                }
                if (completedTask == waitForElectionIntervalChange)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Election timer reset.");
                    break;
                }
                if (completedTask == waitForNodeCountChange)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Node count changed, restart election.");
                    return;
                }
                if (completedTask == waitForNewVoteRequestReply)
                {
                    VoteRequestReply reply = await VoteRequestReplyChannel.Reader.ReadAsync();
                    string replyRoutePrefix = ComputeRoutePrefix(NodeId , reply.ReplierId , reply.ReceiverId);
                    string replyLoggingPrefix = $"<{replyRoutePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";

                    if (reply.ReplierTerm > CurrentTerm)
                    {
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Found higher term, revert to follower role.");
                        CurrentTerm = reply.ReplierTerm;
                        Role = NodeRole.Follower;
                        return;
                    }

                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Processing vote reply.");
                    if (reply.ReplierTerm < CurrentTerm)
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Ignore for vote response from node with lower term {reply.ReplierTerm}.");
                    else if (reply.TermOfRequest != CurrentTerm)
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Ignore for vote response which request at on other term {reply.ReplierTerm}.");
                    else
                    {
                        voteReceived++;
                        if (!reply.VoteGranted)
                            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Being rejected from node {NodeIdToDebugPos[reply.ReplierId]}.");
                        else
                        {
                            voteGranted++;
                            Debug.WriteLine(
                                    "{0} {1}: Vote granted from node {2} ({3}/{4}/{5})." ,
                                    DateTime.Now.TimeOfDay , replyLoggingPrefix ,
                                    NodeIdToDebugPos[reply.ReplierId] , voteGranted , voteReceived , nodeCount
                                );

                            if (voteGranted >= (nodeCount / 2 + 1))
                            {
                                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Enough votes received.");
                                Role = NodeRole.Leader;
                                LeaderId = NodeId;
                                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingPrefix}: Becoming leader.");
                                InitializeLeaderRequiredField();
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    private async Task RunAsFollowerAsync()
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsFollower",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
        Task waitForHeartBeatReceived = receiveHeartBeatTcs.Task;
        Task waitForElectionIntervalChange = changeElectionIntervalTcs.Task;
        Task completedTask = await Task.WhenAny(
                waitForElectionTimerEnd ,
                waitForHeartBeatReceived ,
                waitForElectionIntervalChange
            );
        if (completedTask == waitForElectionTimerEnd)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Election timeout -> becoming candidate.");
            Role = NodeRole.Candidate;
            return;
        }
        if (completedTask == waitForHeartBeatReceived)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Heart beat received, restart election timeout timer.");
            return;
        }
        if (completedTask == waitForElectionIntervalChange)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Election timer reset. Restart as follower.");
            return;
        }

        throw new UnreachableException();
    }

    private async Task RunAsLeaderAsync()
    {
        int nodeIntId = NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsLeader",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: BEGIN");

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Sending append entries request to other nodes.");
        if (AppendEntriesToOtherNodes is null)
            throw new ArgumentNullException(nameof(AppendEntriesToOtherNodes));
        Array.ForEach(
                AppendEntriesToOtherNodes.GetInvocationList() ,
                appendFunctionDelegate =>
                {
                    Func<Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task> appendFunction
                        = (Func<Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>)appendFunctionDelegate;
                    appendFunction.Invoke(NodeId , commitIndex , logEntries , nextIndexes);
                }
            );

        Task waitForHeartBeatTimerEnd = Task.Delay(heartBeatInterval);
        while (true)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Wait for replies of append entries requests.");
            Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
            Task waitForNewAppendEntriesRequestReply = AppendEntriesReplyChannel.Reader.WaitToReadAsync().AsTask();
            Task completedTask = await Task.WhenAny(
                    waitForHeartBeatTimerEnd ,
                    waitForRevertToFollowerSignal ,
                    waitForNewAppendEntriesRequestReply
                );
            if (completedTask == waitForRevertToFollowerSignal)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Revert to follower.");
                (Role , LeaderId) = (NodeRole.Follower , null);
                return;
            }
            if (completedTask == waitForHeartBeatTimerEnd)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Heart beat timer triggered.");
                return;
            }
            if (completedTask == waitForNewAppendEntriesRequestReply)
            {
                AppendEntriesReply reply = await AppendEntriesReplyChannel.Reader.ReadAsync();
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: New reply received from node {NodeIdToDebugPos[reply.ReplierId]}.");

                Guid replyNodeId = reply.ReplierId;
                int replyMatchIndex = reply.MatchIndex;
                if (reply.AppendSuccess)
                    (nextIndexes[replyNodeId] , matchIndexes[replyNodeId]) = (replyMatchIndex + 1 , replyMatchIndex);
                else
                    nextIndexes[replyNodeId] = Math.Max(nextIndexes.GetValueOrDefault(replyNodeId , logEntries.Count) - 1 , 1);

                int newCommitIndex = matchIndexes
                   .Where(
                            (_ , candidateCommitIndex) => matchIndexes.Count(kvp => kvp.Value >= candidateCommitIndex) >= nodeCount / 2
                        )
                   .Where((_ , candidateNextIndex) => logEntries[candidateNextIndex].Term == CurrentTerm)
                   .DefaultIfEmpty(new KeyValuePair<Guid , int>(Guid.Empty , 0))
                   .Max(kvp => kvp.Value);
                if (newCommitIndex != commitIndex)
                {
                    commitIndex = newCommitIndex;
                    commitNewEntryTcs.TrySetResult();
                    commitNewEntryTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingPrefix}: Commit index has been set to {newCommitIndex}.");
            }
        }
    }

    private void WriteCurrentTermToDb(int currentTerm)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand getCurrentTerm = new SqliteCommand(
                    $"""
                     SELECT Key , Value
                     FROM {TableName}
                     WHERE Key = 'CurrentTerm';
                     """ , connection , transaction
                );
            using SqliteDataReader reader = getCurrentTerm.ExecuteReader();

            using SqliteCommand setCurrentTerm = new SqliteCommand(
                    reader.Read()
                        ? $"""
                            UPDATE RaftNodeState
                            SET Value = '{currentTerm}'
                            WHERE Key = 'CurrentTerm';
                            """
                        : $"""
                            INSERT INTO RaftNodeState (Key , Value)
                            VALUES ('CurrentTerm' , '{CurrentTerm}');
                            """ ,
                    connection , transaction
                );
            setCurrentTerm.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot write new entries to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
    }

    private void WriteVoteInfoToDb(Guid voteFor , int termOfVote)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand getVoteFor = new SqliteCommand(
                    $"""
                    SELECT Key
                    FROM {TableName}
                    WHERE Key = 'VoteFor';
                    """ , connection , transaction
                );
            using SqliteDataReader getVoteForReader = getVoteFor.ExecuteReader();
            using SqliteCommand setVoteFor = new SqliteCommand(
                    getVoteForReader.Read()
                        ?  $"""
                            UPDATE {TableName}
                            SET Value = '{voteFor}'
                            WHERE Key = 'VoteFor';
                            """
                        :  $"""
                            INSERT INTO {TableName} (Key , Value)
                            VALUES ('VoteFor' , '{voteFor}');
                            """ ,
                    connection , transaction
                );
            setVoteFor.ExecuteNonQuery();

            using SqliteCommand getTermOfVote = new SqliteCommand(
                    $"""
                    SELECT Key
                    FROM {TableName}
                    WHERE Key = 'TermOfVote';
                    """ , connection , transaction
                );
            using SqliteDataReader getTermOfVoteReader = getTermOfVote.ExecuteReader();
            using SqliteCommand setTermOfVote = new SqliteCommand(
                    getTermOfVoteReader.Read()
                        ? $"""
                            UPDATE {TableName}
                            SET Value = '{termOfVote}'
                            WHERE Key = 'TermOfVote';
                            """
                        : $"""
                            INSERT INTO {TableName} (Key , Value)
                            VALUES ('TermOfVote' , '{termOfVote}');
                            """ ,
                    connection , transaction
                );
            setTermOfVote.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot write new entries to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
    }

    #region Debug Helper
    private string ComputeRoutePrefix(Guid loggingNodeId , Guid? sourceNodeId , Guid? targetNodeId)
    {
        int resultLength = NodeIdToDebugPos.Count * 2 - 1;
        StringBuilder resultBuilder = new StringBuilder(resultLength).Append(' ' , resultLength);

        int sourcePos = sourceNodeId is null ? -1 : NodeIdToDebugPos[sourceNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[sourcePos * 2] = 'O';
        int targetPos = targetNodeId is null ? -1 : NodeIdToDebugPos[targetNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[targetPos * 2] = 'O';
        int loggingPos = NodeIdToDebugPos[loggingNodeId];
        resultBuilder[loggingPos * 2] = 'X';

        /* Drawing arrow */
        if (sourceNodeId is not null && targetNodeId is not null)
        {
            int min = Math.Min(sourcePos * 2 , targetPos * 2) , max = Math.Max(sourcePos * 2 , targetPos * 2);
            for (int i = min + 1; i < max; i++)
                resultBuilder[i] = '-';
            if (targetPos < sourcePos)
                resultBuilder[targetPos * 2 + 1] = '<';
            else
                resultBuilder[targetPos * 2 - 1] = '>';
        }

        return resultBuilder.ToString();
    }
    #endregion
}
