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

    private const string TableName = "RaftNodeState";

    /// <summary> Debug purpose, messing this up has no any effect on Raft behaviour. (except printing invalid debug logs) </summary>
    public readonly Dictionary<Guid , int> NodeIdToDebugPos;

    public readonly Guid NodeId;
    public readonly Channel<AppendEntriesReply> AppendEntriesReplyChannel;
    public readonly Channel<VoteRequestReply> VoteRequestReplyChannel;
    /// <remarks>
    ///     Signature of the function:
    ///     <code> async Task AppendEntriesToOtherNodes(Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList&lt;LogEntry&gt; logEntries , IReadOnlyDictionary&lt;Guid , int&gt; nextIndexes); </code>
    /// </remarks>
    public Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>? AppendEntriesToOtherNodes;
    /// <remarks>
    ///     Signature of the function:
    ///     <code> async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm); </code>
    /// </remarks>
    public Func<Guid , Guid , int , int , Task>? SendVoteRequestToOtherNodes;
    private readonly string dbFilePath;
    private readonly LogEntryList logEntries;
    private readonly TraceSource debugTrace;
    private readonly TraceSource standardTrace;
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


    public RaftNode(int electionTimeOutIntervalIn , int heartBeatIntervalIn , int nodeCountIn , string databaseDirectory = "data" , bool removeExistData = false ,
                    IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null)
        : this(
                Guid.NewGuid() , electionTimeOutIntervalIn , heartBeatIntervalIn , nodeCountIn ,
                databaseDirectory , removeExistData ,
                standardTraceListenersIn , debugTraceListenersIn
            ) { }

    public RaftNode(Guid guid , int electionTimeOutIntervalIn , int heartBeatIntervalIn , int nodeCountIn ,
                    string databaseDirectory = "data" , bool removeExistData = false ,
                    IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null ,
                    Dictionary<Guid , int>? nodeIdToDebugPos = null)
    {
        if (!Directory.Exists(databaseDirectory))
            Directory.CreateDirectory(databaseDirectory);

        NodeId = guid;

        /* Initialize database pragma and schema */
        dbFilePath = $"{databaseDirectory}/{NodeId}.db";
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
        logEntries = new LogEntryList(dbFilePath , NodeId , removeExistData , standardTraceListenersIn , debugTraceListenersIn);

        /* For notifications and messaging */
        changeElectionIntervalTcs  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeNodeCountTcs         = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        revertToFollowerTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiveHeartBeatTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitNewEntryTcs          = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        VoteRequestReplyChannel   = Channel.CreateUnbounded<VoteRequestReply>();
        AppendEntriesReplyChannel = Channel.CreateUnbounded<AppendEntriesReply>();

        /* Setup logging */
        standardTrace = new TraceSource($"{"Raft",-12}.{NodeId}" , SourceLevels.All);  // <--+-< standard trace
        standardTrace.Listeners.Clear();                                               //    |
        foreach (TraceListener listener in standardTraceListenersIn ?? [])             //    |
            standardTrace.Listeners.Add(listener);  // <-------------------------------------+
        debugTrace = new TraceSource($"{"Raft",-12}.{NodeId}" , SourceLevels.All);  // <--+-< debug trace
        debugTrace.Listeners.Clear();                                               //    |
        foreach (TraceListener listener in debugTraceListenersIn ?? [])             //    |
            debugTrace.Listeners.Add(listener);  // <-------------------------------------+
        NodeIdToDebugPos = nodeIdToDebugPos ?? [];
        NodeIdToDebugPos.Add(NodeId , NodeIdToDebugPos.Count);
    }


    #region Major API
    public async Task StartAsync()
    {
        standardTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node start.");
        debugTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node start.");
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

    public void Stop()
    {
        standardTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node stop.");
        debugTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node stop.");
        stopRaftTcs?.TrySetResult();
    }

    public (bool Success , bool KeyFound , object? Value) GetValue(string key)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"GetValue",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN (Key = {key})");

        for (int i = commitIndex; i >= 0; i--)
            if (logEntries[i].Key == key)
                switch (logEntries[i].Operation)
                {
                    case LogEntryOperation.Put:
                        debugTrace.TraceInformation($"{loggingPrefix}: Value founded ({key}: {logEntries[i].GetValue()}).");
                        return (true , true , logEntries[i].GetValue());
                    case LogEntryOperation.Delete:
                        debugTrace.TraceInformation($"{loggingPrefix}: This value (key = {key}) has been deleted.");
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
    ///             <description> The key exist before this propose. Null if there is nothing to propose or wrong node. </description>
    ///         </item>
    ///     </list>
    /// </returns>>
    public async Task<(bool Success , bool WrongNode , bool? KeyFound)> ProposeAsync(LogEntryOperation operation , string? key , object? value)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"Propose",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        /* Ignore new propose when this node is not leader. */
        if (LeaderId != NodeId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Cannot handle new propose because this node is not raft leader.");
            return (false , true , null);
        }

        if (operation == LogEntryOperation.None || key is null)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Nothing to propose.");
            return (true , false , null);
        }

        if (operation == LogEntryOperation.Delete && !CheckKeyExist(key , logEntries.Count))
        {
            debugTrace.TraceInformation($"{loggingPrefix}: No such key to delete.");
            return (false , false , false);
        }

        LogEntry entry = new Int32LogEntry(CurrentTerm , operation , key , (int?)value);
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
                debugTrace.TraceInformation($"{loggingPrefix}: Cannot handle new propose because this node is not raft leader.");
                return (false , true , null);
            }
            if (finishedTask == waitForNewEntriesCommitedSignal && commitIndex >= thisLogIndex)
                switch (operation)
                {
                    case LogEntryOperation.Put:
                        debugTrace.TraceInformation($"{loggingPrefix}: Propose {key} success (put).");
                        return (true , false , CheckKeyExist(key , thisLogIndex));

                    case LogEntryOperation.Delete:
                        debugTrace.TraceInformation($"{loggingPrefix}: Propose {key} success (delete).");
                        return (true , false , true);

                    default: throw new UnreachableException();
                }
        }
    }

    public void SetElectionTimeoutInterval(int electionTimeoutIntervalIn)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"SetElectionTimeout",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN (interval = {electionTimeoutIntervalIn})");

        electionTimeoutInterval = electionTimeoutIntervalIn;
        changeElectionIntervalTcs.TrySetResult();
        changeElectionIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        standardTrace.TraceEvent(TraceEventType.Information , 0 , $"Election timeout interval has been set to {electionTimeoutInterval}");
    }

    public void SetHeartBeatInterval(int heartBeatIntervalIn)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"SetHeartBeatInterval",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN (interval = {heartBeatIntervalIn})");

        heartBeatInterval = heartBeatIntervalIn;
        changeHeartBeatIntervalTcs.TrySetResult();
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        standardTrace.TraceEvent(TraceEventType.Information , 0 , $"Heart beat interval has been set to {heartBeatInterval}");
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
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleVoteRequest",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");
        debugTrace.TraceInformation($"{loggingPrefix}: Handle vote request from node {NodeIdToDebugPos[args.RequesterId]}");
        standardTrace.TraceInformation($"Handle vote request from node {args.RequesterId}");

        VoteRequestReply reply = new VoteRequestReply
        {
            RequestId = args.RequestId ,
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,
            TermOfRequest = args.RequesterTerm ,
            VoteGranted = false ,
        };

        if (args.ReceiverId != NodeId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        /* reject when requester has lower term */
        if (args.RequesterTerm < CurrentTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Requester has lower term ({args.RequesterTerm}), vote request rejected.");
            return reply;
        }

        if (args.RequesterTerm > CurrentTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Requester has higher term ({args.RequesterTerm}).");
            CurrentTerm = args.RequesterTerm;
            reply.ReplierTerm = args.RequesterTerm;

            switch (Role)
            {
                case NodeRole.Follower:
                    debugTrace.TraceInformation($"{loggingPrefix}: Heart beat signal received.");
                    receiveHeartBeatTcs.TrySetResult();
                    receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                case NodeRole.Leader:
                    LeaderId = null;
                    revertToFollowerTcs.TrySetResult();
                    standardTrace.TraceInformation("Higher term found, revert to follower.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Higher term found, revert to follower.");
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                case NodeRole.Candidate:
                    revertToFollowerTcs.TrySetResult();
                    standardTrace.TraceInformation("Higher term found, revert to follower.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Higher term found, revert to follower.");
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                default:
                    throw new UnreachableException();
            }
        }

        /* reject when this node has vote for others */
        if (VoteInfo.TermOfVote == CurrentTerm && VoteInfo.VoteFor != Guid.Empty && VoteInfo.VoteFor != args.RequesterId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: This node has voted for other node ({NodeIdToDebugPos[VoteInfo.VoteFor]}), vote request rejected.");
            return reply;
        }

        if (commitIndex <= args.RequesterLastLogIndex && logEntries[commitIndex].Term <= args.RequesterLastLogTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Vote granted because log of requester is as update as this node.");
            VoteInfo = (args.RequesterId , CurrentTerm);
            return reply with { VoteGranted = true };
        }

        debugTrace.TraceInformation(
                "{0}: By default, reject vote request (thisCommitIndex {1}, otherCommitIndex {2}, thisLastTerm {3}, otherLastTerm {4})." ,
                loggingPrefix , commitIndex , args.RequesterLastLogIndex , logEntries[commitIndex].Term , args.RequesterLastLogTerm
            );
        return reply;
    }

    public AppendEntriesReply HandleAppendEntries(AppendEntriesArgs args)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleAppendEntries",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");
        debugTrace.TraceInformation($"{loggingPrefix}: Handle append entries request from node {NodeIdToDebugPos[args.RequesterId]}");
        standardTrace.TraceInformation($"Handle append entries request from node {args.RequesterId}");

        AppendEntriesReply reply = new AppendEntriesReply
        {
            RequestId = args.RequestId ,
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,

            AppendSuccess = false ,
            MatchIndex = commitIndex ,
        };

        if (args.ReceiverId != NodeId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        if (args.RequesterTerm < CurrentTerm)
        {
            debugTrace.TraceInformation(
                    $"{loggingPrefix}: Append entries request REJECTED because requester has an outdated term (Impl Ref #1)."
                );
            return reply;
        }

        standardTrace.TraceInformation($"Update leader to node {args.RequesterId}");
        debugTrace.TraceInformation($"{loggingPrefix}: Update leader to node {NodeIdToDebugPos[args.RequesterId]}");
        LeaderId = args.RequesterId;

        switch (Role)
        {
            case NodeRole.Follower:
                debugTrace.TraceInformation($"{loggingPrefix}: Heart beat signal received");
                receiveHeartBeatTcs.TrySetResult();
                receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Candidate:
                standardTrace.TraceInformation("Revert to follower (Candidate -> Follower).");
                debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower (Candidate -> Follower).");
                revertToFollowerTcs.TrySetResult();
                revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Leader:
                debugTrace.TraceInformation($"{loggingPrefix}: TODO (Split leader) finish this state (Code navigation key: lm2leockDs3uGiHJ).");
                break;

            default:
                debugTrace.TraceEvent(TraceEventType.Error , 0 , $"{loggingPrefix}: Error, incorrect state (Code navigation key: uKP8WWdVLb2ZlWqN).");
                break;
        }

        standardTrace.TraceInformation($"Updating terms to {args.RequesterTerm}.");
        debugTrace.TraceInformation($"{loggingPrefix}: Updating terms to {args.RequesterTerm}.");
        CurrentTerm = args.RequesterTerm;
        reply.ReplierTerm = CurrentTerm;
        loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleAppendEntries",-20}>";

        if (logEntries.Count <= args.PreviousLogIndex)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Append entries FAILED because some logs are missing prior new entries (Impl Ref #2).");
            return reply;
        }

        if (logEntries[args.PreviousLogIndex].Term != args.PreviousLogTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Remove conflicting logs and subsequent logs (Impl Ref #3).");
            logEntries.RemoveRange(args.PreviousLogIndex , logEntries.Count - args.PreviousLogIndex);
        }

        debugTrace.TraceInformation($"{loggingPrefix}: Append new entries (Impl Ref #4).");
        if (!logEntries.TryEraseAndAppendEntriesAt(args.Entries , args.PreviousLogIndex + 1))
        {
            standardTrace.TraceInformation("Append new entries failed.");
            return reply;
        }

        debugTrace.TraceInformation($"{loggingPrefix}: Update commit index (Impl Ref #5).");
        if (args.LeaderCommit > commitIndex)
            commitIndex = Math.Min(logEntries.Count - 1 , args.LeaderCommit);
        debugTrace.TraceInformation($"{loggingPrefix}: Commit index has been set to {commitIndex}.");

        reply.AppendSuccess = true;
        reply.MatchIndex = args.PreviousLogIndex + args.Entries.Count;
        standardTrace.TraceInformation($"Append entries success (matchIndex = {reply.MatchIndex}).");
        debugTrace.TraceInformation($"{loggingPrefix}: Append entries success (matchIndex = {reply.MatchIndex}).");
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
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"initLeaderReqField",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

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
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsCandidate",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        CurrentTerm++;
        standardTrace.TraceInformation($"Advancing to term {CurrentTerm}.");
        debugTrace.TraceInformation($"{loggingPrefix}: Advancing to term {CurrentTerm}.");
        loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsCandidate",-20}>";

        standardTrace.TraceInformation("Sending vote request to other nodes.");
        debugTrace.TraceInformation($"{loggingPrefix}: Sending vote request to other nodes.");
        VoteInfo = (NodeId , CurrentTerm);
        if (SendVoteRequestToOtherNodes is null)
            throw new ArgumentNullException(nameof(SendVoteRequestToOtherNodes));
        Guid requestId = Guid.NewGuid();
        Array.ForEach(
                SendVoteRequestToOtherNodes.GetInvocationList() ,
                requestFunction => ((Func<Guid , Guid , int , int , Task>)requestFunction).Invoke(requestId , NodeId , commitIndex , logEntries[commitIndex].Term)
            );

        int voteGranted = 1 , voteReceived = 1;
        while (true)
        {
            Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
            standardTrace.TraceInformation($"Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");
            debugTrace.TraceInformation($"{loggingPrefix}: Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");

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
                    standardTrace.TraceInformation("Election timeout, restart election.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Election timeout, restart election.");
                    return;
                }
                if (completedTask == waitForRevertToFollowerSignal)
                {
                    standardTrace.TraceInformation("Revert to follower.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower.");
                    Role = NodeRole.Follower;
                    return;
                }
                if (completedTask == waitForElectionIntervalChange)
                {
                    standardTrace.TraceInformation("Election timer reset.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Election timer reset.");
                    break;
                }
                if (completedTask == waitForNodeCountChange)
                {
                    standardTrace.TraceInformation("Node count changed, restart election.");
                    debugTrace.TraceInformation($"{loggingPrefix}: Node count changed, restart election.");
                    return;
                }
                if (completedTask == waitForNewVoteRequestReply)
                {
                    VoteRequestReply reply = await VoteRequestReplyChannel.Reader.ReadAsync();
                    if (reply.RequestId != requestId)
                    {
                        debugTrace.TraceInformation($"{loggingPrefix}: Old reply received from node {NodeIdToDebugPos[reply.ReplierId]}.");
                        continue;
                    }

                    string replyRoutePrefix = ComputeRoutePrefix(NodeId , reply.ReplierId , reply.ReceiverId);
                    string replyLoggingPrefix = $"<{replyRoutePrefix} Term {CurrentTerm} {"runAsCandidate",-20}>";

                    if (reply.ReplierTerm > CurrentTerm)
                    {
                        standardTrace.TraceInformation("Found higher term, revert to follower role.");
                        debugTrace.TraceInformation($"{replyLoggingPrefix}: Found higher term, revert to follower role.");
                        CurrentTerm = reply.ReplierTerm;
                        Role = NodeRole.Follower;
                        return;
                    }

                    debugTrace.TraceInformation($"{replyLoggingPrefix}: Processing vote reply.");
                    if (reply.ReplierTerm < CurrentTerm)
                        debugTrace.TraceInformation($"{replyLoggingPrefix}: Ignore for vote response from node with lower term {reply.ReplierTerm}.");
                    else if (reply.TermOfRequest != CurrentTerm)
                        debugTrace.TraceInformation($"{replyLoggingPrefix}: Ignore for vote response which request at on other term {reply.ReplierTerm}.");
                    else
                    {
                        voteReceived++;
                        if (!reply.VoteGranted)
                        {
                            standardTrace.TraceInformation($"Being rejected from node {reply.ReplierId}.");
                            debugTrace.TraceInformation($"{replyLoggingPrefix}: Being rejected from node {NodeIdToDebugPos[reply.ReplierId]}.");
                        }
                        else
                        {
                            voteGranted++;
                            debugTrace.TraceInformation(
                                    "{0}: Vote granted from node {1} ({2}/{3}/{4})." ,
                                    replyLoggingPrefix , NodeIdToDebugPos[reply.ReplierId] , voteGranted , voteReceived , nodeCount
                                );

                            if (voteGranted >= (nodeCount / 2 + 1))
                            {
                                standardTrace.TraceInformation("Becoming leader with enough votes received.");
                                debugTrace.TraceInformation($"{loggingPrefix}: Becoming leader with enough votes received.");
                                Role = NodeRole.Leader;
                                LeaderId = NodeId;
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
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsFollower",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

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
            standardTrace.TraceInformation("Election timeout -> becoming candidate.");
            debugTrace.TraceInformation($"{loggingPrefix}: Election timeout -> becoming candidate.");
            Role = NodeRole.Candidate;
            return;
        }
        if (completedTask == waitForHeartBeatReceived)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Heart beat received, restart election timeout timer.");
            return;
        }
        if (completedTask == waitForElectionIntervalChange)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Election timer reset. Restart as follower.");
            return;
        }

        throw new UnreachableException();
    }

    private async Task RunAsLeaderAsync()
    {
        standardTrace.TraceEvent(TraceEventType.Verbose , 0 , "Begin as raft leader.");
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsLeader",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        debugTrace.TraceInformation($"{loggingPrefix}: Sending append entries request to other nodes.");
        if (AppendEntriesToOtherNodes is null)
            throw new ArgumentNullException(nameof(AppendEntriesToOtherNodes));
        Guid requestId = Guid.NewGuid();
        Array.ForEach(
                AppendEntriesToOtherNodes.GetInvocationList() ,
                appendFunctionDelegate =>
                {
                    Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task> appendFunction
                        = (Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>)appendFunctionDelegate;
                    appendFunction.Invoke(requestId , NodeId , commitIndex , logEntries , nextIndexes);
                }
            );

        Task waitForHeartBeatTimerEnd = Task.Delay(heartBeatInterval);
        while (true)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Wait for replies of append entries requests.");
            Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
            Task waitForNewAppendEntriesRequestReply = AppendEntriesReplyChannel.Reader.WaitToReadAsync().AsTask();
            Task completedTask = await Task.WhenAny(
                    waitForHeartBeatTimerEnd ,
                    waitForRevertToFollowerSignal ,
                    waitForNewAppendEntriesRequestReply
                );
            if (completedTask == waitForRevertToFollowerSignal)
            {
                standardTrace.TraceInformation("Revert to follower.");
                debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower.");
                (Role , LeaderId) = (NodeRole.Follower , null);
                return;
            }
            if (completedTask == waitForHeartBeatTimerEnd)
            {
                debugTrace.TraceInformation($"{loggingPrefix}: Heart beat timer triggered.");
                return;
            }
            if (completedTask == waitForNewAppendEntriesRequestReply)
            {
                AppendEntriesReply reply = await AppendEntriesReplyChannel.Reader.ReadAsync();
                if (reply.RequestId != requestId)
                {
                    debugTrace.TraceInformation($"{loggingPrefix}: Old reply received from node {NodeIdToDebugPos[reply.ReplierId]}.");
                    continue;
                }
                debugTrace.TraceInformation($"{loggingPrefix}: New reply received from node {NodeIdToDebugPos[reply.ReplierId]}.");

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
                standardTrace.TraceInformation($"Commit index has been set to {newCommitIndex}.");
                debugTrace.TraceInformation($"{loggingPrefix}: Commit index has been set to {newCommitIndex}.");
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
