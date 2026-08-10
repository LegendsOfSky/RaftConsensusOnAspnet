using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Utils;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class RaftNode
{
    public Guid? LeaderId { get; private set; }
    public int CurrentTerm { get; private set; }
    public IReadOnlyList<LogEntry> LogEntries => logEntries;

    public static Dictionary<Guid , int> S_nodeIdToDebugPos = new Dictionary<Guid , int>();

    public readonly Guid NodeId;
    public NodeRole Role;
    public Func<Guid , int , IReadOnlyList<LogEntry> , IReadOnlyDictionary<Guid , int> , Task<bool>> AppendEntriesToOtherNodes;
    public Channel<AppendEntriesReply> AppendEntriesReplyChannel;
    public Func<Guid , int , int , Task<bool>> SendVoteRequestToOtherNodes;
    public Channel<VoteRequestReply> VoteRequestReplyChannel;

    private readonly List<LogEntry> logEntries;
    private int nodeCount;
    private Guid? voteFor;
    private int? votingTerm;
    private int electionTimeoutInterval;
    private int heartBeatInterval;
    private int commitIndex;
    private int lastApplied;
    private Dictionary<Guid , int> nextIndecies;
    private Dictionary<Guid , int> matchIndecies;
    private readonly Channel<bool> stopSignalChannel = Channel.CreateBounded<bool>(1);

    private event EventHandler? OnNodeCountChangedEvent;
    private event EventHandler? RevertToFollowerEvent;
    private event EventHandler? OnElectionIntervalChangedEvent;
    private event EventHandler? OnHeartBeatIntervalChangedEvent;
    private event EventHandler? OnHeartBeatReceivedEvent;


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

        S_nodeIdToDebugPos.Add(NodeId , S_nodeIdToDebugPos.Count);
        VoteRequestReplyChannel = Channel.CreateUnbounded<VoteRequestReply>();
        AppendEntriesReplyChannel = Channel.CreateUnbounded<AppendEntriesReply>();
    }
    

    public async Task StopAsync() => await stopSignalChannel.Writer.WriteAsync(true);

    public async Task StartAsync()
    {
        logEntries.Add(new LogEntry
        {
            Term = 0 ,
        });
        while (true)
        {
            if (stopSignalChannel.Reader.Count != 0 && await stopSignalChannel.Reader.ReadAsync())
                return;

            switch (Role)
            {
                case NodeRole.Follower:  await RunAsFollowerAsync();  break;
                case NodeRole.Candidate: await RunAsCandidateAsync(); break;
                case NodeRole.Leader:    await RunAsLeaderAsync();    break;
                default: throw new UnreachableException();
            }
        }
    }

    public void SetElectionTimeoutInterval(int electionTimeoutIntervalIn)
    {
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetElectionTimeout",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN (interval = {electionTimeoutIntervalIn})");

        electionTimeoutInterval = electionTimeoutIntervalIn;
        OnElectionIntervalChangedEvent?.Invoke(this , EventArgs.Empty);
    }

    public void SetHeartBeatInterval(int heartBeatIntervalIn)
    {
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"SetHeartBeatInterval",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN (interval = {heartBeatIntervalIn})");

        heartBeatInterval = heartBeatIntervalIn;
        OnHeartBeatIntervalChangedEvent?.Invoke(this , EventArgs.Empty);
    }

    public void UpdateRaftClusterNodeCount(int nodeCountIn)
    {
        nodeCount = nodeCountIn;
        OnNodeCountChangedEvent?.Invoke(this , EventArgs.Empty);
    }

    public VoteRequestReply HandleVoteRequest(VoteRequestArgs args)
    {
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleVoteRequest",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Handle vote request from node {nodeIntId}");

        if (args.ReceiverId != NodeId)
        {
            throw new NotImplementedException();
        }

        VoteRequestReply reply = new VoteRequestReply()
        {
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,
            VoteGranted = false ,
        };

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
                    OnHeartBeatReceivedEvent?.Invoke(this , EventArgs.Empty);
                    break;

                case NodeRole.Leader:
                    LeaderId = null;
                    goto case NodeRole.Candidate;
                case NodeRole.Candidate:
                    Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Higher term found, revert to follower.");
                    RevertToFollowerEvent?.Invoke(this , EventArgs.Empty);
                    break;

                default:
                    throw new UnreachableException();
            }
        }

        /* reject when this node has vote for others */
        if (votingTerm == CurrentTerm && voteFor != null && voteFor != args.RequesterId)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: This node has voted for other node ({S_nodeIdToDebugPos[voteFor.Value]}), vote request rejected.");
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
                DateTime.Now.TimeOfDay ,loggingprefix ,
                commitIndex , args.RequesterLastLogIndex , logEntries[commitIndex].Term , args.RequesterLastLogTerm
            );
        return reply;
    }

    public AppendEntriesReply HandleAppendEntries(AppendEntriesArgs args)
    {
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , args.ReceiverId , args.RequesterId);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"HandleAppendEntries",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Handle append entries request from node {nodeIntId}");

        if (args.ReceiverId != NodeId)
        {
            throw new NotImplementedException();
        }

        AppendEntriesReply reply = new AppendEntriesReply
        {
            ReceiverId = args.RequesterId ,
            ReplierId = NodeId ,
            ReplierTerm = CurrentTerm ,

            AppendSuccess = false ,
            MatchIndex = commitIndex ,
        };

        if (args.RequesterTerm < CurrentTerm)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append entries request REJECTED because requester has an outdated term (Impl Ref #1).");
            return reply;
        }

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Update leader to node {S_nodeIdToDebugPos[args.RequesterId]}");
        LeaderId = args.RequesterId;

        switch (Role)
        {
            case NodeRole.Follower:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Heart beat signal received");
                OnHeartBeatReceivedEvent?.Invoke(this , EventArgs.Empty);
                break;

            case NodeRole.Candidate:
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Revert to follower (Candidate -> Follower).");
                RevertToFollowerEvent?.Invoke(this , EventArgs.Empty);
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
        for (int i = 0; i < args.Entries.Count; i++)
        {
            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Append entry {i} to {logEntries.Count}");
            logEntries.Add(args.Entries[i]);
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
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsFollower",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        TaskCompletionSource waitForHeartBeatReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EventHandler? waitForHeartBeatReceivedEvent = null;
        waitForHeartBeatReceivedEvent = (_ , _) =>
        {
            OnHeartBeatReceivedEvent -= waitForHeartBeatReceivedEvent;
            waitForHeartBeatReceivedTcs.TrySetResult();
        };
        OnHeartBeatReceivedEvent += waitForHeartBeatReceivedEvent;

        TaskCompletionSource waitForElectionIntervalChangeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EventHandler? waitForElectionIntervalChangeEvent = null;
        waitForElectionIntervalChangeEvent = (_ , _) =>
        {
            OnElectionIntervalChangedEvent -= waitForElectionIntervalChangeEvent;
            waitForElectionIntervalChangeTcs.TrySetResult();
        };
        OnElectionIntervalChangedEvent += waitForElectionIntervalChangeEvent;

        Task waitForElectionTimerEnd = Task.Delay(electionTimeoutInterval);
        Task waitForHeartBeatReceived = waitForHeartBeatReceivedTcs.Task;
        Task waitForElectionIntervalChange = waitForElectionIntervalChangeTcs.Task;
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
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
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
                TaskCompletionSource waitForRevertToFollowerSignalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                EventHandler? waitForRevertToFollowerSignalEvent = null;
                waitForRevertToFollowerSignalEvent = (_ , _) =>
                {
                    RevertToFollowerEvent -= waitForRevertToFollowerSignalEvent;
                    waitForRevertToFollowerSignalTcs.TrySetResult();
                };
                RevertToFollowerEvent += waitForRevertToFollowerSignalEvent;

                TaskCompletionSource waitForElectionIntervalChangeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                EventHandler? waitForElectionIntervalChangeEvent = null;
                waitForElectionIntervalChangeEvent = (_ , _) =>
                {
                    OnElectionIntervalChangedEvent -= waitForElectionIntervalChangeEvent;
                    waitForElectionIntervalChangeTcs.TrySetResult();
                };
                OnElectionIntervalChangedEvent += waitForElectionIntervalChangeEvent;

                Task waitForRevertToFollowerSignal = waitForRevertToFollowerSignalTcs.Task;
                Task waitForElectionIntervalChange = waitForElectionIntervalChangeTcs.Task;
                Task<VoteRequestReply> waitForNewVoteRequestReply = VoteRequestReplyChannel.Reader.ReadAsync().AsTask();
                Task completedTask = await Task.WhenAny(
                        waitForElectionTimerEnd ,
                        waitForRevertToFollowerSignal ,
                        waitForElectionIntervalChange ,
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
                    {
                        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Ignore for votes from previous term {reply.ReplierTerm}.");
                    }
                    else
                    {
                        voteReceived++;
                        if (!reply.VoteGranted)
                        {
                            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {replyLoggingprefix}: Being rejected from node {S_nodeIdToDebugPos[reply.ReplierId]}.");
                        }
                        else
                        {
                            voteGranted++;
                            Debug.WriteLine(
                                    "{0} {1}: Vote granted from node {2} ({3}/{4}/{5})." ,
                                    DateTime.Now.TimeOfDay , replyLoggingprefix ,
                                    S_nodeIdToDebugPos[reply.ReplierId] , voteGranted , voteReceived , nodeCount
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
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"runAsLeader",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Sending append entries request to other nodes.");
        AppendEntriesToOtherNodes?.Invoke(NodeId , commitIndex , logEntries , nextIndecies);

        Task waitForHeartBeatTimerEnd = Task.Delay(heartBeatInterval);
        while (true)
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            Task<AppendEntriesReply> waitForNewAppendEntriesRequestReply = AppendEntriesReplyChannel.Reader.ReadAsync(cts.Token).AsTask();

            TaskCompletionSource waitForRevertToFollowerSignalTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            EventHandler? waitForRevertToFollowerSignalEvent = null;
            waitForRevertToFollowerSignalEvent = (_ , _) =>
            {
                RevertToFollowerEvent -= waitForRevertToFollowerSignalEvent;
                waitForRevertToFollowerSignalTcs.TrySetResult();
            };
            RevertToFollowerEvent += waitForRevertToFollowerSignalEvent;

            Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Wait for replis of append entries requests.");
            Task completedTask = await Task.WhenAny(
                    waitForHeartBeatTimerEnd ,
                    waitForRevertToFollowerSignalTcs.Task ,
                    waitForNewAppendEntriesRequestReply
                );
            await cts.CancelAsync();
            if (completedTask == waitForRevertToFollowerSignalTcs.Task)
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
                AppendEntriesReply reply = waitForNewAppendEntriesRequestReply.Result;
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: New reply received from node {S_nodeIdToDebugPos[reply.ReceiverId]}.");

                Guid replyNodeId = reply.ReplierId;
                int replyMatchIndex = reply.MatchIndex;
                if (reply.AppendSuccess)
                    (nextIndecies[replyNodeId] , matchIndecies[replyNodeId]) = (replyMatchIndex + 1 , replyMatchIndex);
                else
                    nextIndecies[replyNodeId] = Math.Max(nextIndecies.GetValueOrDefault(replyNodeId , logEntries.Count) - 1 , 1);

                commitIndex = nextIndecies.Where(
                        (_ , queryNextIndex) => queryNextIndex >= 0 && nextIndecies.Count(kvp => kvp.Value >= queryNextIndex) >= nextIndecies.Count / 2
                    ).Max(kvp => kvp.Value);
                Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: Commit index has been set to {commitIndex}.");
            }
        }
    }

    private void InitializeLeaderRequiredField()
    {
        int nodeIntId = S_nodeIdToDebugPos[NodeId];
        string routePrefix = ComputeRoutePrefix(NodeId , null , null);
        string loggingprefix = $"<{routePrefix} Node {nodeIntId} Term {CurrentTerm} {"initLeaderReqField",-20}>";
        Debug.WriteLine($"{DateTime.Now.TimeOfDay} {loggingprefix}: BEGIN");

        nextIndecies = new Dictionary<Guid , int>(nodeCount)
        {
            [Guid.Empty] = logEntries.Count  ,
        };
        matchIndecies = new Dictionary<Guid , int>(nodeCount);
    }

    private static string ComputeRoutePrefix(Guid loggingNodeId , Guid? sourceNodeId , Guid? targetNodeId)
    {
        int resultLength = S_nodeIdToDebugPos.Count * 2 - 1;
        StringBuilder resultBuilder = new StringBuilder(resultLength).Append(' ' , resultLength);

        int sourcePos = sourceNodeId is null ? -1 : S_nodeIdToDebugPos[sourceNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[sourcePos * 2] = 'O';
        int targetPos = targetNodeId is null ? -1 : S_nodeIdToDebugPos[targetNodeId.Value];
        if (sourceNodeId is not null)
            resultBuilder[targetPos * 2] = 'O';
        int loggingPos = S_nodeIdToDebugPos[loggingNodeId];
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
        S_nodeIdToDebugPos.Remove(NodeId);
    }
}
