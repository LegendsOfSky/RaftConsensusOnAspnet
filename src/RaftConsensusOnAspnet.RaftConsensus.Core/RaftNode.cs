using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class RaftNode
{
    public int CurrentTerm
    {
        get => File.Exists($"{NodeId}.Term") && int.TryParse(File.ReadAllText($"{NodeId}.Term") , out int result) ? result : 0;
        private set => File.WriteAllText($"{NodeId}.Term" , value.ToString());
    }
    public Guid? LeaderId { get; private set; }
    public IReadOnlyList<LogEntry> LogEntries => logEntries;

    /// <summary> Debug purpose, messing this up has no any effect on Raft behaviour. (except printing invalid debug logs) </summary>
    public static Dictionary<Guid , int> S_NodeIdToDebugPos = new Dictionary<Guid , int>();
    public readonly Guid NodeId;
    public readonly Channel<AppendEntriesReply> AppendEntriesReplyChannel;
    public readonly Channel<VoteRequestReply> VoteRequestReplyChannel;
    public NodeRole Role;
    public Func<Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task<bool>> AppendEntriesToOtherNodes;
    public Func<Guid , int , int , Task<bool>> SendVoteRequestToOtherNodes;

    private readonly List<LogEntry> logEntries;
    private int nodeCount;
    private int electionTimeoutInterval;
    private int heartBeatInterval;
    private int commitIndex;
    private int lastApplied;
    private int? votingTerm;
    private Guid? voteFor;
    private Dictionary<Guid , int> nextIndecies;
    private Dictionary<Guid , int> matchIndecies;
    private TaskCompletionSource? stopRaftTcs;
    private TaskCompletionSource changeElectionIntervalTcs;
    private TaskCompletionSource changeHeartBeatIntervalTcs;
    private TaskCompletionSource changeNodeCountTcs;
    private TaskCompletionSource revertToFollowerTcs;
    private TaskCompletionSource receiveHeartBeatTcs;
    private TaskCompletionSource commitNewEntryTcs;


    public RaftNode(int electionTimeOutIntervalIn , int heartBeatIntervalIn , int nodeCountIn)
    {
        /* Identity (Volatile) */
        NodeId = Guid.NewGuid();
        Role = NodeRole.Follower;

        /* Raft cluster info (Volatile) */
        nodeCount = nodeCountIn;

        /* Persistent state */
        CurrentTerm = 0;
        logEntries = new List<LogEntry>();

        /* Volatile state */
        commitIndex = 0;
        lastApplied = 0;
        nextIndecies = new Dictionary<Guid , int>(nodeCount);
        matchIndecies = new Dictionary<Guid , int>(nodeCount);
        electionTimeoutInterval = electionTimeOutIntervalIn;
        heartBeatInterval = heartBeatIntervalIn;

        S_NodeIdToDebugPos.Add(NodeId , S_NodeIdToDebugPos.Count);
        VoteRequestReplyChannel = Channel.CreateUnbounded<VoteRequestReply>();
        AppendEntriesReplyChannel = Channel.CreateUnbounded<AppendEntriesReply>();

        changeElectionIntervalTcs  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeHeartBeatIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changeNodeCountTcs         = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        revertToFollowerTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiveHeartBeatTcs        = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitNewEntryTcs          = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }


    public void Stop() => stopRaftTcs?.TrySetResult();

    public async Task StartAsync()
    {
        stopRaftTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        logEntries.Add(new LogEntry { Term = 0 });
        while (true)
        {
            if (stopRaftTcs.Task.IsCompleted)
                return;

            switch (Role)
            {
                case NodeRole.Follower:  await RunAsFollowerAsync();  break;
                case NodeRole.Candidate: await RunAsCandidateAsync(); break;
                case NodeRole.Leader:    await RunAsLeaderAsync();    break;
                default:                 throw new UnreachableException();
            }
        }
    }

    public void SetElectionTimeoutInterval(int electionTimeoutIntervalIn)
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetElectionTimeout",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN (interval = {electionTimeoutIntervalIn})");

        electionTimeoutInterval = electionTimeoutIntervalIn;
        changeElectionIntervalTcs.TrySetResult();
        changeElectionIntervalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void SetHeartBeatInterval(int heartBeatIntervalIn)
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetHeartBeatInterval",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN (interval = {heartBeatIntervalIn})");

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

    public async Task<(bool Success , bool WrongNode , bool? KeyFound)> ProposeAsync(LogEntryOperation operation , string? key , object? value)
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"Propose",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        /* Ignore new propose when this node is not leader. */
        if (LeaderId != NodeId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Cannot handle new propose because this node is not raft leader.");
            return (false , true , null);
        }

        if (operation == LogEntryOperation.None || key is null)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Nothing to propose.");
            return (true , false , null);
        }

        if (operation == LogEntryOperation.Delete && !CheckKeyExist(key , logEntries.Count))
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: No such key to delete.");
            return (false , false , false);
        }

        LogEntry entry = new LogEntry { Term = CurrentTerm , Operation = operation , Key = key , Value = value };
        logEntries.Add(entry);
        int thisLogIndex = logEntries.IndexOf(entry);
        while (true)
        {
            Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
            Task waitForNewEntriesCommitedSiganl = commitNewEntryTcs.Task;
            Task finishedTask = await Task.WhenAny(
                    waitForRevertToFollowerSignal ,
                    waitForNewEntriesCommitedSiganl
                );
            if (finishedTask == waitForRevertToFollowerSignal)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Cannot handle new propose because this node is not raft leader.");
                return (false , true , null);
            }
            if (finishedTask == waitForNewEntriesCommitedSiganl && commitIndex >= thisLogIndex)
                switch (operation)
                {
                    case LogEntryOperation.Put:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Propose {key} success (put).");
                        return (true , false , CheckKeyExist(key , thisLogIndex));

                    case LogEntryOperation.Delete:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Propose {key} success (delete).");
                        return (true , false , true);

                    default: throw new UnreachableException();
                }
        }
    }

    public (bool Success , bool KeyFound , object? Value) GetValue(string key)
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"GetValue",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        for (int i = commitIndex; i >= 0; i--)
            if (logEntries[i].Key == key)
                switch (logEntries[i].Operation)
                {
                    case LogEntryOperation.Put:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Value founded ({key}: {logEntries[i].Value}).");
                        return (true , true , logEntries[i].Value);
                    case LogEntryOperation.Delete:
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: This value (key = {key}) has been deleted.");
                        return (true , false , null);
                }

        return (false , false , null);
    }

    public VoteRequestReply HandleVoteRequest(VoteRequestArgs args)
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleVoteRequest",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Handle vote request from node {nodeIntId}");

        VoteRequestReply reply = new VoteRequestReply()
        {
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,
            VoteGranted = false ,
        };

        if (args.ReceiverId != NodeId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        /* reject when requester has lower term */
        if (args.RequesterTerm < CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Requester has lower term ({args.RequesterTerm}), vote request rejected.");
            return reply;
        }

        if (args.RequesterTerm > CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Requester has higher term ({args.RequesterTerm}).");
            CurrentTerm = args.RequesterTerm;
            reply.ReplierTerm = args.RequesterTerm;

            switch (Role)
            {
                case NodeRole.Follower:
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Heart beat signal received. (TODO)");
                    receiveHeartBeatTcs.TrySetResult();
                    receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                case NodeRole.Leader:
                    LeaderId = null;
                    goto case NodeRole.Candidate;
                case NodeRole.Candidate:
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Higher term found, revert to follower.");
                    revertToFollowerTcs.TrySetResult();
                    revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    break;

                default:
                    throw new UnreachableException();
            }
        }

        /* reject when this node has vote for others */
        if (votingTerm == CurrentTerm && voteFor != null && voteFor != args.RequesterId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: This node has voted for other node ({S_NodeIdToDebugPos[voteFor.Value]}), vote request rejected.");
            return reply;
        }

        if (commitIndex <= args.RequesterLastLogIndex && logEntries[commitIndex].Term <= args.RequesterLastLogTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Vote granted because log of requester is as update as this node.");
            voteFor = args.RequesterId;
            votingTerm = CurrentTerm;
            return reply with { VoteGranted = true };
        }

        Debug.WriteLine(
                "{0} {1}: By default, reject vote request (thisCommitIndex {2}, otherCommitIndex {3}, thisLastTerm {4}, otherLastTerm {5})." ,
                DateTime.Now.TimeOfDay , loggingprefix ,
                commitIndex , args.RequesterLastLogIndex , logEntries[commitIndex].Term , args.RequesterLastLogTerm
            );
        return reply;
    }

    public AppendEntriesReply HandleAppendEntries(AppendEntriesArgs args)
    {
        int nodeIntId = S_NodeIdToDebugPos[args.RequesterId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleAppendEntries",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Handle append entries request from node {nodeIntId}");

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
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Incorrect receiver. Request will be ignored.");
            return reply;
        }

        if (args.RequesterTerm < CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append entries request REJECTED because requester has an outdated term (Impl Ref #1).");
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Update leader to node {S_NodeIdToDebugPos[args.RequesterId]}");
        LeaderId = args.RequesterId;

        switch (Role)
        {
            case NodeRole.Follower:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Heart beat signal received");
                receiveHeartBeatTcs.TrySetResult();
                receiveHeartBeatTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Candidate:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Revert to follower (Candidate -> Follower).");
                revertToFollowerTcs.TrySetResult();
                revertToFollowerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                break;

            case NodeRole.Leader:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: TODO finish this state (Code navigation key: lm2leockDs3uGiHJ).");
                break;

            default:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Error, incorrect state (Code navigation key: uKP8WWdVLb2ZlWqN).");
                break;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Updating terms to {args.RequesterTerm}.");
        CurrentTerm = args.RequesterTerm;
        reply.ReplierTerm = CurrentTerm;
        loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleAppendEntries",-20}>";

        if (logEntries.Count <= args.PreviousLogIndex)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append entries FAILED because some logs are missing prior new entries (Impl Ref #2).");
            return reply;
        }

        if (logEntries[args.PreviousLogIndex].Term != args.PreviousLogTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Remove conflicting logs and subsequent logs (Impl Ref #3).");
            logEntries.RemoveRange(args.PreviousLogIndex , logEntries.Count - args.PreviousLogIndex);
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append new entries (Impl Ref #4).");
        if (!AppendEntries(args.Entries , args.PreviousLogIndex + 1))
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append new entries failed.");
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Update commit index (Impl Ref #5).");
        if (args.LeaderCommit > commitIndex)
            commitIndex = Math.Min(logEntries.Count - 1 , args.LeaderCommit);
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Commit index has been set to {commitIndex}.");

        reply.AppendSuccess = true;
        reply.MatchIndex = args.PreviousLogIndex + args.Entries.Count;
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append entries success (matchIndex = {reply.MatchIndex}).");
        return reply;
    }

    private async Task RunAsFollowerAsync()
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsFollower",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

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
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Election timeout -> becoming candidate.");
            Role = NodeRole.Candidate;
            return;
        }
        if (completedTask == waitForHeartBeatReceived)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Heart beat received, restart election timeout timer.");
            return;
        }
        if (completedTask == waitForElectionIntervalChange)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Election timer reset. Restart as follower. (temporary logic, may need fix).");
            return;
        }

        throw new UnreachableException();
    }

    private async Task RunAsCandidateAsync()
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        CurrentTerm++;
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Advancing to term {CurrentTerm}.");
        loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";

        (voteFor , votingTerm) = (NodeId , CurrentTerm);
        SendVoteRequestToOtherNodes?.Invoke(NodeId , commitIndex , logEntries[commitIndex].Term);

        int voteGranted = 1 , voteReceived = 1;
        while (true)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Start waiting for vote replies until {electionTimeoutInterval} of election timer runs out.");

            Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
            while (true)
            {
                Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
                Task waitForElectionIntervalChange = changeElectionIntervalTcs.Task;
                Task waitForNodeCountChange = changeNodeCountTcs.Task;
                Task<VoteRequestReply> waitForNewVoteRequestReply = VoteRequestReplyChannel.Reader.ReadAsync().AsTask();
                Task completedTask = await Task.WhenAny(
                        waitForElectionTimerEnd ,
                        waitForRevertToFollowerSignal ,
                        waitForElectionIntervalChange ,
                        waitForNodeCountChange ,
                        waitForNewVoteRequestReply
                    );
                if (completedTask == waitForElectionTimerEnd)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Election timeout, restart election.");
                    return;
                }
                if (completedTask == waitForRevertToFollowerSignal)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Revert to follower.");
                    Role = NodeRole.Follower;
                    return;
                }
                if (completedTask == waitForElectionIntervalChange)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Election timer reset.");
                    break;
                }
                if (completedTask == waitForNodeCountChange)
                {
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Node count changed, restart election.");
                    return;
                }
                if (completedTask == waitForNewVoteRequestReply)
                {
                    VoteRequestReply reply = await waitForNewVoteRequestReply;
                    string replyRoutePrefix = ComputeRoutePrefix(NodeId , reply.ReplierId , reply.ReceiverId);
                    string replyLoggingprefix = $"<{replyRoutePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsCandidate",-20}>";

                    if (reply.ReplierTerm > CurrentTerm)
                    {
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Found higher term, revert to follower role.");
                        CurrentTerm = reply.ReplierTerm;
                        Role = NodeRole.Follower;
                        return;
                    }

                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Processing vote reply.");
                    if (reply.ReplierTerm < CurrentTerm)
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Ignore for votes from previous term {reply.ReplierTerm}.");
                    else
                    {
                        voteReceived++;
                        if (!reply.VoteGranted)
                            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Being rejected from node {S_NodeIdToDebugPos[reply.ReplierId]}.");
                        else
                        {
                            voteGranted++;
                            Debug.WriteLine(
                                    "{0} {1}: Vote granted from node {2} ({3}/{4}/{5})." ,
                                    DateTime.Now.TimeOfDay , replyLoggingprefix ,
                                    S_NodeIdToDebugPos[reply.ReplierId] , voteGranted , voteReceived , nodeCount
                                );

                            if (voteGranted >= (nodeCount / 2 + 1))
                            {
                                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Enough votes received.");
                                Role = NodeRole.Leader;
                                LeaderId = NodeId;
                                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Becoming leader.");
                                InitializeLeaderRequiredField();
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    private async Task RunAsLeaderAsync()
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsLeader",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Sending append entries request to other nodes.");
        AppendEntriesToOtherNodes?.Invoke(NodeId , commitIndex , logEntries , nextIndecies);

        Task waitForHeartBeatTimerEnd = Task.Delay(heartBeatInterval);
        while (true)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Wait for replis of append entries requests.");
            Task waitForRevertToFollowerSignal = revertToFollowerTcs.Task;
            Task waitForNewAppendEntriesRequestReply = AppendEntriesReplyChannel.Reader.WaitToReadAsync().AsTask();
            Task completedTask = await Task.WhenAny(
                    waitForHeartBeatTimerEnd ,
                    waitForRevertToFollowerSignal ,
                    waitForNewAppendEntriesRequestReply
                );
            if (completedTask == waitForRevertToFollowerSignal)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Revert to follower.");
                (Role , LeaderId) = (NodeRole.Follower , null);
                return;
            }
            if (completedTask == waitForHeartBeatTimerEnd)
            {
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Heart beat timer triggered.");
                return;
            }
            if (completedTask == waitForNewAppendEntriesRequestReply)
            {
                AppendEntriesReply reply = await AppendEntriesReplyChannel.Reader.ReadAsync();
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: New reply received from node {S_NodeIdToDebugPos[reply.ReceiverId]}.");

                Guid replyNodeId = reply.ReplierId;
                int replyMatchIndex = reply.MatchIndex;
                if (reply.AppendSuccess)
                    (nextIndecies[replyNodeId] , matchIndecies[replyNodeId]) = (replyMatchIndex + 1 , replyMatchIndex);
                else
                    nextIndecies[replyNodeId] = Math.Max(nextIndecies.GetValueOrDefault(replyNodeId , logEntries.Count) - 1 , 1);

                int newCommitIndex = matchIndecies
                   .Where(
                            (_ , candidateCommitIndex) => matchIndecies.Count(kvp => kvp.Value >= candidateCommitIndex) >= nodeCount / 2
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
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Commit index has been set to {newCommitIndex}.");
            }
        }
    }

    private void InitializeLeaderRequiredField()
    {
        int nodeIntId = S_NodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"initLeaderReqField",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        nextIndecies = new Dictionary<Guid , int>(nodeCount) { [Guid.Empty] = logEntries.Count };
        matchIndecies = new Dictionary<Guid , int>(nodeCount);
    }

    private bool AppendEntries(IReadOnlyList<LogEntry> entriesToAppend , int startIndex)
    {
        if (logEntries.Count > startIndex)
            logEntries.RemoveRange(startIndex , logEntries.Count - startIndex);
        else if (logEntries.Count < startIndex)
            throw new InvalidOperationException("Append entries failed because of missing logs.");

        logEntries.AddRange(entriesToAppend);
        return true;
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

    private static string ComputeRoutePrefix(Guid loggingNodeId , Guid? sourceNodeId , Guid? targetNodeId)
    {
        int resultLength = S_NodeIdToDebugPos.Count * 2 - 1;
        StringBuilder resultBuilder = new StringBuilder(resultLength).Append(' ' , resultLength);

        int sourcePos = sourceNodeId is null ? -1 : S_NodeIdToDebugPos[sourceNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[sourcePos * 2] = 'O';
        int targetPos = targetNodeId is null ? -1 : S_NodeIdToDebugPos[targetNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[targetPos * 2] = 'O';
        int loggingPos = S_NodeIdToDebugPos[loggingNodeId];
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


    ~RaftNode()
    {
        S_NodeIdToDebugPos.Remove(NodeId);
    }
}
