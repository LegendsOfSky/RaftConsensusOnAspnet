using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using System.Text;
using System.Threading.Channels;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

internal class NetworkConnection
{
    public RaftNode Source;
    public RaftNode Target;
    public bool HasMismatchMessage;
    public Channel<MessagePackageBase> ExpectedMessagesChannel = Channel.CreateUnbounded<MessagePackageBase>();
    public List<(MessagePackageBase Expected , MessagePackageBase Actual , DateTime Time)> DebugLogs = [];


    public NetworkConnection(RaftNode source , RaftNode target) => (Source , Target) = (source , target);


    public async Task HandleAppendEntriesAsync(
        Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
    {
        int nextIndex = nextIndexes.TryGetValue(Target.NodeId , out int i) ? i : nextIndexes[Guid.Empty];
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

        if (ExpectedMessagesChannel.Reader.Count > 0)
        {
            MessagePackageBase expectSendPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectSendPackageBase is not AppendEntriesSendPackage expectSendPackage)
            {
                DebugLogs.Add((expectSendPackageBase , actualSendPackage , DateTime.Now));
                Console.WriteLine(
                        "Mismatch message on {0} <- {1}: AppendEntriesSendPackage sent, but test case expect {2} ({3})" ,
                        Target.NodeIdToDebugPos[Target.NodeId] , Source.NodeIdToDebugPos[Source.NodeId] ,
                        expectSendPackageBase.GetType() , DateTime.Now.TimeOfDay
                    );
                HasMismatchMessage = true;
            }
            else
            {
                (actualSendPackage.Delay , actualSendPackage.MessageDropped) = (expectSendPackage.Delay , expectSendPackage.MessageDropped);
                DebugLogs.Add((expectSendPackage , actualSendPackage , DateTime.Now));
                if (expectSendPackage.Delay != 0)
                    Console.WriteLine(
                            "{0} BEGIN (Delay = {1}, Time = {2})" ,
                            MessagePackageToString(
                                    Source.NodeIdToDebugPos , Source.NodeIdToDebugPos[args.RequesterId] , Source.NodeIdToDebugPos[args.ReceiverId] ,
                                    actualSendPackage
                                ) ,
                            expectSendPackage.Delay , DateTime.Now.TimeOfDay
                        );
                await Task.Delay(expectSendPackage.Delay);
                Console.WriteLine(
                        "{0}{1}" ,
                        MessagePackageToString(
                                Source.NodeIdToDebugPos , Source.NodeIdToDebugPos[args.RequesterId] , Source.NodeIdToDebugPos[args.ReceiverId] ,
                                actualSendPackage
                            ) ,
                        expectSendPackage.Delay != 0
                            ? $" ARRIVE (Delay = {expectSendPackage.Delay}, Time = {DateTime.Now.TimeOfDay})"
                            : ""
                    );
                if (expectSendPackage.MessageDropped)
                    return;
            }
        }

        AppendEntriesReply reply = Target.HandleAppendEntries(args);  // forward request to actual raft node

        if (ExpectedMessagesChannel.Reader.Count > 0)
        {
            AppendEntriesReceivePackage actualRecvPackage = new AppendEntriesReceivePackage
                {
                Term = reply.ReplierTerm ,

                Success = reply.AppendSuccess ,
                MatchIndex = reply.MatchIndex ,
            };

            MessagePackageBase expectRecvPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectRecvPackageBase is not AppendEntriesReceivePackage expectRecvPackage)
            {
                DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
                Console.WriteLine(
                        "Mismatch message on {0} -> {1}: AppendEntriesReceivePackage sent, but test case expect {2} ({3})" ,
                        Target.NodeIdToDebugPos[Target.NodeId] , Source.NodeIdToDebugPos[Source.NodeId] ,
                        expectRecvPackageBase.GetType() , DateTime.Now.TimeOfDay
                    );
                HasMismatchMessage = true;
            }
            else
            {
                if (expectRecvPackageBase.Delay != 0)
                    Console.WriteLine(
                            "{0} BEGIN (Delay = {1}, Time = {2})" ,
                            MessagePackageToString(
                                    Target.NodeIdToDebugPos , Target.NodeIdToDebugPos[reply.ReceiverId] , Target.NodeIdToDebugPos[reply.ReplierId] ,
                                    actualRecvPackage
                                ) ,
                            expectRecvPackageBase.Delay , DateTime.Now.TimeOfDay
                        );
                await Task.Delay(expectRecvPackageBase.Delay);
                (actualRecvPackage.Delay , actualRecvPackage.MessageDropped) = (expectRecvPackage.Delay , expectRecvPackage.MessageDropped);
                DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
                Console.WriteLine(
                        "{0}{1}" ,
                        MessagePackageToString(
                                Target.NodeIdToDebugPos , Target.NodeIdToDebugPos[reply.ReceiverId] , Target.NodeIdToDebugPos[reply.ReplierId] ,
                                actualRecvPackage
                            ) ,
                        expectRecvPackageBase.Delay != 0
                            ? $" ARRIVE (Delay = {expectRecvPackageBase.Delay}, Time = {DateTime.Now.TimeOfDay}) "
                            : ""
                    );
                if (!expectRecvPackageBase.MessageDropped)
                    await Source.AppendEntriesReplyChannel.Writer.WriteAsync(reply);
            }
        }
    }

    public async Task HandleVoteRequestAsync(Guid requesterId , int commitIndex , int previousLogTerm)
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

        if (ExpectedMessagesChannel.Reader.Count > 0)
        {
            MessagePackageBase expectSendPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectSendPackageBase is not VoteRequestSendPackage expectSendPackage)
            {
                DebugLogs.Add((expectSendPackageBase , actualSendPackage , DateTime.Now));
                Console.WriteLine(
                        "Mismatch message on {0} <- {1}: VoteRequestSendPackage sent, but test case expect {2} ({3})" ,
                        Target.NodeIdToDebugPos[Target.NodeId] , Source.NodeIdToDebugPos[Source.NodeId] ,
                        expectSendPackageBase.GetType() , DateTime.Now.TimeOfDay
                    );
                HasMismatchMessage = true;
            }
            else
            {
                (actualSendPackage.Delay , actualSendPackage.MessageDropped) = (expectSendPackage.Delay , expectSendPackage.MessageDropped);
                DebugLogs.Add((expectSendPackage , actualSendPackage , DateTime.Now));
                if (expectSendPackage.Delay != 0)
                    Console.WriteLine(
                            "{0} BEGIN (Delay = {1}, Time = {2})" ,
                            MessagePackageToString(
                                    Source.NodeIdToDebugPos , Source.NodeIdToDebugPos[args.RequesterId] , Source.NodeIdToDebugPos[args.ReceiverId] ,
                                    actualSendPackage
                                ) ,
                            expectSendPackage.Delay , DateTime.Now.TimeOfDay
                        );
                await Task.Delay(expectSendPackage.Delay);
                Console.WriteLine(
                        "{0}{1}" ,
                        MessagePackageToString(
                                Source.NodeIdToDebugPos , Source.NodeIdToDebugPos[args.RequesterId] , Source.NodeIdToDebugPos[args.ReceiverId] ,
                                actualSendPackage
                            ) ,
                        expectSendPackage.Delay != 0
                            ? $" ARRIVE (Delay = {expectSendPackage.Delay}, Time = {DateTime.Now.TimeOfDay})"
                            : ""
                    );
                if (expectSendPackage.MessageDropped)
                    return;
            }
        }

        VoteRequestReply reply = Target.HandleVoteRequest(args);  // forward request to actual raft node

        if (ExpectedMessagesChannel.Reader.Count > 0)
        {
            VoteRequestReceivePackage actualRecvPackage = new VoteRequestReceivePackage
                {
                Term = reply.ReplierTerm ,
                Granted = reply.VoteGranted ,
            };
            MessagePackageBase expectRecvPackageBase = await ExpectedMessagesChannel.Reader.ReadAsync();
            if (expectRecvPackageBase is not VoteRequestReceivePackage expectRecvPackage)
            {
                DebugLogs.Add((expectRecvPackageBase , actualRecvPackage , DateTime.Now));
                Console.WriteLine(
                        "Mismatch message on {0} -> {1}: VoteRequestReceivePackage sent, but test case expect {2} ({3})" ,
                        Target.NodeIdToDebugPos[Target.NodeId] , Source.NodeIdToDebugPos[Source.NodeId] ,
                        expectRecvPackageBase.GetType() , DateTime.Now.TimeOfDay
                    );
                HasMismatchMessage = true;
                await Source.VoteRequestReplyChannel.Writer.WriteAsync(reply);
            }
            else
            {
                if (expectRecvPackage.Delay != 0)
                    Console.WriteLine(
                            "{0} BEGIN (Delay = {1}, Time = {2})" ,
                            MessagePackageToString(
                                    Source.NodeIdToDebugPos , Source.NodeIdToDebugPos[reply.ReceiverId] , Source.NodeIdToDebugPos[reply.ReplierId] ,
                                    actualRecvPackage
                                ) ,
                            expectRecvPackage.Delay , DateTime.Now.TimeOfDay
                        );
                await Task.Delay(expectRecvPackage.Delay);
                (actualRecvPackage.Delay , actualRecvPackage.MessageDropped) = (expectRecvPackage.Delay , expectRecvPackage.MessageDropped);
                DebugLogs.Add((expectRecvPackage , actualRecvPackage , DateTime.Now));
                Console.WriteLine(
                        "{0}{1}" ,
                        MessagePackageToString(
                                Target.NodeIdToDebugPos , Target.NodeIdToDebugPos[reply.ReceiverId] , Target.NodeIdToDebugPos[reply.ReplierId] ,
                                actualRecvPackage
                            ) ,
                        expectRecvPackage.Delay != 0
                            ? $" ARRIVE (Delay = {expectRecvPackage.Delay}, Time = {DateTime.Now.TimeOfDay})"
                            : ""
                    );
                if (!expectRecvPackage.MessageDropped)
                    await Source.VoteRequestReplyChannel.Writer.WriteAsync(reply);
            }
        }
    }

    public static string MessagePackageToString(RaftNode source , RaftNode target , MessagePackageBase message)
    => MessagePackageToString(source.NodeIdToDebugPos , source.NodeIdToDebugPos[source.NodeId] , target.NodeIdToDebugPos[target.NodeId] , message);

    public static string MessagePackageToString(Dictionary<Guid , int> nodeIdToDebugPos , int sourceId , int targetId , MessagePackageBase message)
    {
        return message switch {
            VoteRequestSendPackage voteSend
                => voteSend.MessageDropped
                    ? $"node {sourceId}: dropped RequestVote to {targetId}"
                    : string.Format(
                            "node {0} <- {1}: RequestVote -- term: {2}, candidateId: {3}, lastLogIdx: {4}, lastLogTerm: {5}" ,
                            targetId , sourceId , voteSend.Term , sourceId , voteSend.LastLogIndex , voteSend.LastLogTerm
                        ),

            VoteRequestReceivePackage voteRecv
                => voteRecv.MessageDropped
                    ? $"node {targetId}: dropped RequestVoteResponse to {sourceId}"
                    : $"node {targetId} -> {sourceId}: {(voteRecv.Granted ? "granted" : "reject")}, term: {voteRecv.Term}",

            AppendEntriesSendPackage appendSend
                => appendSend.MessageDropped
                    ? $"node {sourceId}: dropped AppendEntries to {targetId}"
                    : string.Format(
                            "node {0} <- {1}: AppendEntries -- term: {2}, leaderId: {3}, prevLogIdx: {4}, prevLogTerm: {5}, entries: [{6}], leaderCommit: {7}" ,
                            targetId , sourceId , appendSend.Term ,
                            nodeIdToDebugPos[appendSend.LeaderId] , appendSend.PreviousLogIndex , appendSend.PreviousLogTerm ,
                            new StringBuilder().AppendJoin(' ' , appendSend.Entries.Select(entry => entry.ToString())) , appendSend.LeaderCommit
                        ),

            AppendEntriesReceivePackage appendRecv
                => appendRecv.MessageDropped
                    ? $"node {targetId}: dropped AppendEntriesResponse to {sourceId}"
                    : $"node {targetId} -> {sourceId}: {(appendRecv.Success ? "success" : "failed")}, term: {appendRecv.Term}, matchIdx: {appendRecv.MatchIndex}",

            _ => "",
        };
    }
}
