using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Data;
using RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;
using RaftConsensusOnAspnet.RaftConsensus.Core.Miscs;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class RaftNode
{
    public ChannelWriter<AppendEntriesReply> AppendEntriesReplyChannelWriter => appendEntriesReplyChannel.Writer;
    public int CurrentTerm
    {
        get;
        private set
        {
            WriteCurrentTermToDb(value);
            field = ReadCurrentTermFromDb();
        }
    }
    public Guid? LeaderId { get; private set; }
    public IReadOnlyList<LogEntry> LogEntries => persistentLogEntriesStorage;
    public Guid NodeId { get; }
    /// <summary> Debug purpose, messing this up has no any effect on Raft behavior. (except printing incorrect node indicators on logs) </summary>
    public Dictionary<Guid , int> NodeIdToDebugPos
    {
        get;
        set
        {
            if (stopRaftTcs is not null && !stopRaftTcs.Task.IsCompleted)
                throw new InvalidOperationException();
            field = value;
        }
    }
    public NodeRole Role { get; private set; }
    /// <remarks>
    ///     Signature of the function:
    ///     <code>
    ///         async Task SendAppendEntriesRequestsToOtherNodes(Guid requestId , Guid requesterId , int commitIndex ,
    ///                                                          IReadOnlyList&lt;LogEntry&gt; logEntries , IReadOnlyDictionary&lt;Guid , int&gt; nextIndexes);
    ///     </code>
    /// </remarks>
    public Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>? SendAppendEntriesRequestsToOtherNodes
    {
        get;
        set
        {
            if (stopRaftTcs is not null && !stopRaftTcs.Task.IsCompleted)
                throw new InvalidOperationException();
            field = value;
        }
    }
    /// <remarks>
    ///     Signature of the function:
    ///     <code> async Task SendVoteRequestToOtherNodes(Guid requestId , Guid requesterId , int lastLogIndex , int lastLogTerm); </code>
    /// </remarks>
    public Func<Guid , Guid , int , int , Task>? SendVoteRequestToOtherNodes
    {
        get;
        set
        {
            if (stopRaftTcs is not null && !stopRaftTcs.Task.IsCompleted)
                throw new InvalidOperationException();
            field = value;
        }
    }
    public ChannelWriter<VoteRequestReply> VoteRequestReplyChannelWriter => voteRequestReplyChannel.Writer;
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

    private readonly Channel<AppendEntriesReply> appendEntriesReplyChannel;
    private readonly string dbFilePath;
    private readonly TraceSource debugTrace;
    private readonly ILogEntriesPersistentStorage persistentLogEntriesStorage;
    private readonly TraceSource standardTrace;
    private readonly Channel<VoteRequestReply> voteRequestReplyChannel;
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


    public RaftNode(RaftNodeOptions options , RaftNodeTraces traces)
    {
        if (!Directory.Exists(options.DatabaseDirectory))
            Directory.CreateDirectory(options.DatabaseDirectory);

        NodeId = options.NodeId ?? Guid.NewGuid();

        /* Initialize database pragma and schema */
        dbFilePath = $"{options.DatabaseDirectory}/{NodeId}.db";
        CreateDatabaseSchemaIfNotExist();

        #region Initialize basic raft fields/properties
        Role = NodeRole.Follower;
        nodeCount = options.NodeCount;
        commitIndex = 0;
        (nextIndexes , matchIndexes) = ([] , []);
        (electionTimeoutInterval , heartBeatInterval) = (options.ElectionTimeOutInterval , options.HeartBeatInterval);
        persistentLogEntriesStorage = options.CustomLogEntriesPersistentStorage
            ?? new LogEntriesSqliteStorage(dbFilePath , NodeId , options.RemoveExistData , traces.StandardTraceListeners , traces.DebugTraceListeners);
        if (!options.RemoveExistData)
            CurrentTerm = ReadCurrentTermFromDb();
        else
        {
            CurrentTerm = 0;
            VoteInfo = (Guid.Empty , 0);
        }
        #endregion

        #region Initialize fields for intra-node notification and inter-node messaging
        changeElectionIntervalTcs  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeNodeCountTcs         = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        revertToFollowerTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiveHeartBeatTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitNewEntryTcs          = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        voteRequestReplyChannel   = Channel.CreateUnbounded<VoteRequestReply>();
        appendEntriesReplyChannel = Channel.CreateUnbounded<AppendEntriesReply>();
        #endregion

        #region Setup logging related stuff
        standardTrace = new TraceSource($"{"Raft",-12}.{NodeId}" , SourceLevels.All);  // <--+-< standard trace
        standardTrace.Listeners.Clear();                                               //    |
        foreach (TraceListener listener in traces.StandardTraceListeners ?? [])        //    |
            standardTrace.Listeners.Add(listener);  // <-------------------------------------+
        debugTrace = new TraceSource($"{"Raft",-12}.{NodeId}" , SourceLevels.All);  // <--+-< debug trace
        debugTrace.Listeners.Clear();                                               //    |
        foreach (TraceListener listener in traces.DebugTraceListeners ?? [])        //    |
            debugTrace.Listeners.Add(listener);  // <-------------------------------------+
        NodeIdToDebugPos = traces.NodeIdToDebugPos ?? [];
        if (!NodeIdToDebugPos.ContainsKey(NodeId))
            NodeIdToDebugPos.Add(NodeId , NodeIdToDebugPos.Count);
        #endregion
    }


    #region Major API
    public async Task StartAsync()
    {
        standardTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node start.");
        debugTrace.TraceEvent(TraceEventType.Start , 0 , "Raft node start.");
        stopRaftTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (persistentLogEntriesStorage.Count == 0)
            persistentLogEntriesStorage.Add(new LogEntry { Term = 0 });
        while (!stopRaftTcs.Task.IsCompleted)
            switch (Role)
            {
                case NodeRole.Follower:  await RunAsFollowerAsync();  break;
                case NodeRole.Candidate: await RunAsCandidateAsync(); break;
                case NodeRole.Leader:    await RunAsLeaderAsync();    break;
                default:                 throw new UnreachableException();
            }
        standardTrace.TraceEvent(TraceEventType.Stop , 0 , "Raft node stop.");
        debugTrace.TraceEvent(TraceEventType.Stop , 0 , "Raft node stop.");
    }

    public void Stop() => stopRaftTcs?.TrySetResult();

    public (bool Success , bool KeyFound , object? Value) GetValue(string key)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"GetValue",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN (Key = {key})");

        for (int i = commitIndex; i >= 0; i--)
            if (persistentLogEntriesStorage[i].Key == key)
                switch (persistentLogEntriesStorage[i].Operation)
                {
                    case LogEntryOperation.Put:
                        debugTrace.TraceInformation($"{loggingPrefix}: Value founded ({key}: {persistentLogEntriesStorage[i].GetValue()}).");
                        return (true , true , persistentLogEntriesStorage[i].GetValue());
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
    /// </returns>
    public async Task<(bool Success , bool WrongNode , bool? KeyFound)> ProposeAsync(LogEntryOperation operation , string? key , object? value)
    {
        Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;

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

        if (operation == LogEntryOperation.Delete && !CheckKeyExist(key , persistentLogEntriesStorage.Count))
        {
            debugTrace.TraceInformation($"{loggingPrefix}: No such key to delete.");
            return (false , false , false);
        }

        Task waitForNewEntriesCommitedSignal = commitNewEntryTcs.Task;
        LogEntry entry = new Int32LogEntry(CurrentTerm , operation , key , (int?)value);
        persistentLogEntriesStorage.Add(entry);
        int thisLogIndex = persistentLogEntriesStorage.Index().First(kvp => kvp.Item.Equals(entry)).Index;
        while (true)
        {
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

    #region Inter-node API
    public VoteRequestReply HandleVoteRequest(VoteRequestArgs args)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleVoteRequest",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");
        debugTrace.TraceInformation($"{loggingPrefix}: Handle vote request from node {GetDebugNodeIntId(args.RequesterId)}");
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

        /* ignore request when receiver is incorrect */
        if (args.ReceiverId != NodeId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Incorrect receiver (actual: {args.ReceiverId}). Request will be ignored.");
            return reply;
        }

        /* reject when requester has lower term */
        if (args.RequesterTerm < CurrentTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Requester has lower term ({args.RequesterTerm}), vote request rejected.");
            return reply;
        }

        if (args.RequesterTerm > CurrentTerm)
            AdjustNodeStateInVoteRequest(args  , reply , loggingPrefix);

        /* reject when this node has vote for others */
        if (VoteInfo.TermOfVote == CurrentTerm && VoteInfo.VoteFor != Guid.Empty && VoteInfo.VoteFor != args.RequesterId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: This node has voted for other node ({GetDebugNodeIntId(VoteInfo.VoteFor)}), vote request rejected.");
            return reply;
        }

        if (commitIndex <= args.RequesterLastLogIndex && persistentLogEntriesStorage[commitIndex].Term <= args.RequesterLastLogTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Vote granted because log of requester is as update as this node.");
            VoteInfo = (args.RequesterId , CurrentTerm);
            return reply with { VoteGranted = true };
        }

        debugTrace.TraceInformation(
                "{0}: By default, reject vote request (thisCommitIndex {1}, otherCommitIndex {2}, thisLastTerm {3}, otherLastTerm {4})." ,
                loggingPrefix , commitIndex , args.RequesterLastLogIndex , persistentLogEntriesStorage[commitIndex].Term , args.RequesterLastLogTerm
            );
        return reply;
    }

    public AppendEntriesReply HandleAppendEntries(AppendEntriesArgs args)
    {
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleAppendEntries",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");
        debugTrace.TraceInformation($"{loggingPrefix}: Handle append entries request from node {GetDebugNodeIntId(args.RequesterId)}");
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

        /* ignore request when receiver is incorrect */
        if (args.ReceiverId != NodeId)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Incorrect receiver (actual: {args.ReceiverId}). Request will be ignored.");
            return reply;
        }

        /* reject when requester has lower term */
        if (args.RequesterTerm < CurrentTerm)
        {
            debugTrace.TraceInformation(
                    $"{loggingPrefix}: Append entries request REJECTED because requester has an outdated term (Impl Ref #1)."
                );
            return reply;
        }

        standardTrace.TraceInformation($"Update leader to node {args.RequesterId}");
        debugTrace.TraceInformation($"{loggingPrefix}: Update leader to node {GetDebugNodeIntId(args.RequesterId)}");
        LeaderId = args.RequesterId;

        AdjustNodeStateInAppendEntriesRequest(args , loggingPrefix);

        standardTrace.TraceInformation($"Updating terms to {args.RequesterTerm}.");
        debugTrace.TraceInformation($"{loggingPrefix}: Updating terms to {args.RequesterTerm}.");
        CurrentTerm = args.RequesterTerm;
        reply.ReplierTerm = CurrentTerm;
        loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"HandleAppendEntries",-20}>";

        /* reply append failed when prior log is missing */
        if (persistentLogEntriesStorage.Count <= args.PreviousLogIndex)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Append entries FAILED because some logs are missing prior new entries (Impl Ref #2).");
            return reply;
        }

        /* reply append failed when previous log has incorrect term */
        if (persistentLogEntriesStorage[args.PreviousLogIndex].Term != args.PreviousLogTerm)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Mismatch previous log.");
            return reply;
        }

        debugTrace.TraceInformation($"{loggingPrefix}: Remove conflicting logs and subsequent logs (Impl Ref #3).");
        debugTrace.TraceInformation($"{loggingPrefix}: Append new entries (Impl Ref #4).");
        if (!persistentLogEntriesStorage.TryOverrideEntriesStartFromAndEraseFutureEntries(args.Entries , args.PreviousLogIndex + 1))
        {
            standardTrace.TraceInformation("Append new entries failed.");
            return reply;
        }

        debugTrace.TraceInformation($"{loggingPrefix}: Update commit index (Impl Ref #5).");
        if (args.LeaderCommit > commitIndex)
            commitIndex = Math.Min(persistentLogEntriesStorage.Count - 1 , args.LeaderCommit);
        debugTrace.TraceInformation($"{loggingPrefix}: Commit index has been set to {commitIndex}.");

        reply.AppendSuccess = true;
        reply.MatchIndex = args.PreviousLogIndex + args.Entries.Count;
        standardTrace.TraceInformation($"Append entries success (matchIndex = {reply.MatchIndex}).");
        debugTrace.TraceInformation($"{loggingPrefix}: Append entries success (matchIndex = {reply.MatchIndex}).");
        return reply;
    }
    #endregion

    private void AdjustNodeStateInAppendEntriesRequest(AppendEntriesArgs args , string loggingPrefix)
    {
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
                if (args.RequesterTerm == CurrentTerm)
                    debugTrace.TraceInformation($"{loggingPrefix}: TODO (Split leader) finish this state (Code navigation key: lm2leockDs3uGiHJ).");
                else  // args.RequesterTerm > CurrentTerm
                {
                    standardTrace.TraceInformation("Revert to follower (Leader -> Follower).");
                    debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower (Leader -> Follower).");
                    revertToFollowerTcs.TrySetResult();
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                break;

            default:
                debugTrace.TraceEvent(TraceEventType.Error , 0 , $"{loggingPrefix}: Error, incorrect state (Code navigation key: uKP8WWdVLb2ZlWqN).");
                break;
        }
    }

    private void AdjustNodeStateInVoteRequest(VoteRequestArgs args , VoteRequestReply reply , string loggingPrefix)
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

    private bool CheckKeyExist(string key , int checkBeforeIndex)
    {
        while (--checkBeforeIndex >= 0)
            if (persistentLogEntriesStorage[checkBeforeIndex].Key == key)
                switch (persistentLogEntriesStorage[checkBeforeIndex].Operation)
                {
                    case LogEntryOperation.Put:    return true;
                    case LogEntryOperation.Delete: return false;
                }
        return false;
    }

    private void CreateDatabaseSchemaIfNotExist()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand createTable = connection.CreateCommand();
        createTable.CommandText = $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                Key   TEXT PRIMARY KEY ,
                Value TEXT
            );
            """;
        createTable.ExecuteNonQuery();
    }

    private void InitCandidateSignalReceiver(
        out Task raftStopSignal , out Task waitForElectionIntervalChange , out Task waitForNodeCountChange , out Task waitForRevertToFollowerSignal)
    {
        raftStopSignal = stopRaftTcs?.Task ?? throw new InvalidOperationException();
        waitForElectionIntervalChange = changeElectionIntervalTcs.Task;
        waitForNodeCountChange = changeNodeCountTcs.Task;
        waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
    }

    private void InitLeaderRequiredField()
    {
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"initLeaderReqField",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        nextIndexes = new Dictionary<Guid , int>(nodeCount) { [Guid.Empty] = persistentLogEntriesStorage.Count };
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
        InitCandidateSignalReceiver(
                out Task raftStopSignal , out Task waitForElectionIntervalChange , out Task waitForNodeCountChange , out Task waitForRevertToFollowerSignal
            );

        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsCandidate",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        CurrentTerm++;
        standardTrace.TraceInformation($"Advancing to term {CurrentTerm}.");
        debugTrace.TraceInformation($"{loggingPrefix}: Advancing to term {CurrentTerm}.");
        loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsCandidate",-20}>";

        Guid requestId = SendVoteRequests(loggingPrefix);
        int voteGranted = 1 , voteReceived = 1;
        while (true)
        {
            Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
            standardTrace.TraceInformation($"Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");
            debugTrace.TraceInformation($"{loggingPrefix}: Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");

            while (true)
            {
                Task waitForNewVoteRequestReply = voteRequestReplyChannel.Reader.WaitToReadAsync().AsTask();
                Task completedTask = await Task.WhenAny(
                        raftStopSignal , waitForElectionTimerEnd , waitForRevertToFollowerSignal ,
                        waitForElectionIntervalChange , waitForNodeCountChange , waitForNewVoteRequestReply
                    );

                if (TryProcessPreemptiveTaskWhenRunAsCandidate(
                        completedTask , raftStopSignal , waitForElectionTimerEnd  , waitForRevertToFollowerSignal ,
                        loggingPrefix
                    )) { return; }

                if (TryProcessNonPreemptiveTaskWhenRunAsCandidate(completedTask , waitForElectionIntervalChange  , waitForNodeCountChange , loggingPrefix))
                {
                    InitCandidateSignalReceiver(
                            out raftStopSignal , out waitForElectionIntervalChange , out waitForNodeCountChange , out waitForRevertToFollowerSignal
                        );
                    break;
                }

                if (completedTask == waitForNewVoteRequestReply)
                {
                    VoteRequestReply reply = await voteRequestReplyChannel.Reader.ReadAsync();

                    /* ignore old reply */
                    if (reply.RequestId != requestId || reply.TermOfRequest != CurrentTerm)
                    {
                        debugTrace.TraceInformation($"{loggingPrefix}: Old reply received from node {GetDebugNodeIntId(reply.ReplierId)}.");
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

                    if (reply.ReplierTerm < CurrentTerm)
                    {
                        debugTrace.TraceInformation($"{replyLoggingPrefix}: Ignore for vote response from node with lower term {reply.ReplierTerm}.");
                        continue;
                    }

                    debugTrace.TraceInformation($"{replyLoggingPrefix}: Processing vote reply.");
                    voteReceived++;
                    if (!reply.VoteGranted)
                    {
                        standardTrace.TraceInformation($"Being rejected from node {reply.ReplierId}.");
                        debugTrace.TraceInformation($"{replyLoggingPrefix}: Being rejected from node {GetDebugNodeIntId(reply.ReplierId)}.");
                    }
                    else
                    {
                        voteGranted++;
                        debugTrace.TraceInformation(
                                "{0}: Vote granted from node {1} ({2}/{3}/{4})." ,
                                replyLoggingPrefix , GetDebugNodeIntId(reply.ReplierId) , voteGranted , voteReceived , nodeCount
                            );

                        if (voteGranted >= nodeCount / 2 + 1)
                        {
                            standardTrace.TraceInformation("Becoming leader with enough votes received.");
                            debugTrace.TraceInformation($"{loggingPrefix}: Becoming leader with enough votes received.");
                            Role = NodeRole.Leader;
                            LeaderId = NodeId;
                            InitLeaderRequiredField();
                            return;
                        }
                    }
                }
            }
        }
    }

    private async Task RunAsFollowerAsync()
    {
        Task raftStopSignal = stopRaftTcs?.Task ?? throw new InvalidOperationException();

        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsFollower",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
        Task waitForHeartBeatReceived = receiveHeartBeatTcs.Task;
        Task waitForElectionIntervalChange = changeElectionIntervalTcs.Task;
        Task completedTask = await Task.WhenAny(
                raftStopSignal ,
                waitForElectionTimerEnd ,
                waitForHeartBeatReceived ,
                waitForElectionIntervalChange
            );
        if (completedTask == raftStopSignal)
            return;
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
        Task raftStopSignal = stopRaftTcs?.Task ?? throw new InvalidOperationException();
        Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;

        standardTrace.TraceEvent(TraceEventType.Verbose , 0 , "Begin as raft leader.");
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingPrefix = $"<{routePrefix} Term {CurrentTerm} {"runAsLeader",-20}>";
        debugTrace.TraceEvent(TraceEventType.Verbose , 0 , $"{loggingPrefix}: BEGIN");

        Guid requestId = SendAppendEntriesRequests(loggingPrefix);
        while (true)
        {
            Task waitForHeartBeatIntervalChanged = changeHeartBeatIntervalTcs.Task;
            Task waitForHeartBeatTimerEnd = Task.Delay(heartBeatInterval);
            while (true)
            {
                debugTrace.TraceInformation($"{loggingPrefix}: Wait for replies of append entries requests.");
                Task waitForNewAppendEntriesRequestReply = appendEntriesReplyChannel.Reader.WaitToReadAsync().AsTask();
                Task completedTask = await Task.WhenAny(
                        raftStopSignal ,
                        waitForHeartBeatTimerEnd ,
                        waitForHeartBeatIntervalChanged ,
                        waitForRevertToFollowerSignal ,
                        waitForNewAppendEntriesRequestReply
                    );

                if (TryProcessPreemptiveTaskWhenRunAsLeader(completedTask , raftStopSignal , waitForRevertToFollowerSignal  , waitForHeartBeatTimerEnd , loggingPrefix))
                    return;
                if (completedTask == waitForHeartBeatIntervalChanged)
                    break;

                if (completedTask == waitForNewAppendEntriesRequestReply)
                {
                    AppendEntriesReply reply = await appendEntriesReplyChannel.Reader.ReadAsync();

                    if (reply.MatchIndex >= persistentLogEntriesStorage.Count || reply.MatchIndex < 0)
                    {
                        debugTrace.TraceInformation($"{loggingPrefix}: Received reply with match index outside of possible values. This reply will be ignored.");
                        standardTrace.TraceInformation("Received reply with match index outside of possible values. This reply will be ignored.");
                        continue;
                    }

                    if (reply.RequestId != requestId)
                    {
                        debugTrace.TraceInformation($"{loggingPrefix}: Old reply received from node {GetDebugNodeIntId(reply.ReplierId)}.");
                        continue;
                    }
                    debugTrace.TraceInformation($"{loggingPrefix}: New reply received from node {GetDebugNodeIntId(reply.ReplierId)}.");

                    Guid replyNodeId = reply.ReplierId;
                    int replyMatchIndex = reply.MatchIndex;
                    if (reply.AppendSuccess)
                        (nextIndexes[replyNodeId] , matchIndexes[replyNodeId]) = (replyMatchIndex + 1 , replyMatchIndex);
                    else
                        nextIndexes[replyNodeId] = Math.Max(nextIndexes.GetValueOrDefault(replyNodeId , persistentLogEntriesStorage.Count) - 1 , 1);

                    int newCommitIndex = matchIndexes
                        .Where(
                            candidateMatchIndex => matchIndexes.Count(kvp => kvp.Value >= candidateMatchIndex.Value) >= nodeCount / 2
                            )
                        .Where(candidateNextIndex => persistentLogEntriesStorage[candidateNextIndex.Value].Term == CurrentTerm)
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
    }

    private Guid SendAppendEntriesRequests(string loggingPrefix)
    {
        Guid requestId = Guid.NewGuid();
        debugTrace.TraceInformation($"{loggingPrefix}: Sending append entries request to other nodes.");
        if (SendAppendEntriesRequestsToOtherNodes is null)
            throw new ArgumentNullException(nameof(SendAppendEntriesRequestsToOtherNodes));
        Array.ForEach(
                SendAppendEntriesRequestsToOtherNodes.GetInvocationList() ,
                appendFunctionDelegate =>
                {
                    Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task> appendFunction
                        = (Func<Guid , Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task>)appendFunctionDelegate;
                    appendFunction.Invoke(requestId , NodeId , commitIndex , persistentLogEntriesStorage , nextIndexes);
                }
            );
        return requestId;
    }

    private Guid SendVoteRequests(string loggingPrefix)
    {
        Guid requestId = Guid.NewGuid();
        standardTrace.TraceInformation("Sending vote request to other nodes.");
        debugTrace.TraceInformation($"{loggingPrefix}: Sending vote request to other nodes.");
        VoteInfo = (NodeId , CurrentTerm);
        if (SendVoteRequestToOtherNodes is null)
            throw new ArgumentNullException(nameof(SendVoteRequestToOtherNodes));
        Array.ForEach(
                SendVoteRequestToOtherNodes.GetInvocationList() ,
                requestFunction
                    => ((Func<Guid , Guid , int , int , Task>)requestFunction).Invoke(requestId , NodeId , commitIndex , persistentLogEntriesStorage[commitIndex].Term)
            );
        return requestId;
    }

    private bool TryProcessNonPreemptiveTaskWhenRunAsCandidate(
        Task completedTask , Task waitForElectionIntervalChange , Task waitForNodeCountChange , string loggingPrefix)
    {
        if (completedTask == waitForElectionIntervalChange)
        {
            standardTrace.TraceInformation("Election timer reset.");
            debugTrace.TraceInformation($"{loggingPrefix}: Election timer reset.");
            return true;
        }
        if (completedTask == waitForNodeCountChange)
        {
            standardTrace.TraceInformation("Node count changed, restart election.");
            debugTrace.TraceInformation($"{loggingPrefix}: Node count changed, restart election.");
            return true;
        }
        return false;
    }

    private bool TryProcessPreemptiveTaskWhenRunAsCandidate(
        Task completedTask , Task raftStopSignal , Task waitForElectionTimerEnd , Task waitForRevertToFollowerSignal , string loggingPrefix)
    {
        if (completedTask == raftStopSignal)
            return true;
        if (completedTask == waitForElectionTimerEnd)
        {
            standardTrace.TraceInformation("Election timeout, restart election.");
            debugTrace.TraceInformation($"{loggingPrefix}: Election timeout, restart election.");
            return true;
        }
        if (completedTask == waitForRevertToFollowerSignal)
        {
            standardTrace.TraceInformation("Revert to follower.");
            debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower.");
            Role = NodeRole.Follower;
            return true;
        }

        return false;
    }

    private bool TryProcessPreemptiveTaskWhenRunAsLeader(
        Task completedTask , Task raftStopSignal , Task waitForRevertToFollowerSignal , Task waitForHeartBeatTimerEnd , string loggingPrefix)
    {
        if (completedTask == raftStopSignal)
            return true;
        if (completedTask == waitForRevertToFollowerSignal)
        {
            standardTrace.TraceInformation("Revert to follower.");
            debugTrace.TraceInformation($"{loggingPrefix}: Revert to follower.");
            (Role , LeaderId) = (NodeRole.Follower , null);
            return true;
        }
        if (completedTask == waitForHeartBeatTimerEnd)
        {
            debugTrace.TraceInformation($"{loggingPrefix}: Heart beat timer triggered.");
            return true;
        }
        return false;
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
        bool parameterExists = NodeIdToDebugPos.ContainsKey(loggingNodeId)
            && (sourceNodeId is null || NodeIdToDebugPos.ContainsKey(sourceNodeId.Value))
            && (targetNodeId is null || NodeIdToDebugPos.ContainsKey(targetNodeId.Value));
        if (!parameterExists)
            return "X";

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
        if (sourceNodeId is not null && targetNodeId is not null && sourceNodeId != targetNodeId)
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

    private string GetDebugNodeIntId(Guid nodeId)
        => NodeIdToDebugPos.TryGetValue(nodeId , out int nodeIntId)
            ? nodeIntId.ToString()
            : "unknow";
    #endregion
}
