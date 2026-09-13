using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Messaging;

public class AppendEntriesRequestSender
{
    public const string ClientName = "appendEntriesClient";
    public string Destination;
    public Guid ReceiverId;
    public RaftNode HostNode;
    private readonly IHttpClientFactory httpClientFactory;


    public AppendEntriesRequestSender(IHttpClientFactory httpClientFactoryIn , RaftNode hostNode , string destination , Guid receiverId)
    {
        httpClientFactory = httpClientFactoryIn;
        HostNode = hostNode;
        Destination = destination;
        ReceiverId = receiverId;
    }


    public async Task SendAppendEntriesRequestAsync(
        Guid requestId , Guid requesterId , int commitIndex , IReadOnlyList<LogEntry> logEntries , IReadOnlyDictionary<Guid , int> nextIndexes)
    {
        HttpClient client = httpClientFactory.CreateClient(ClientName);
        int nextIndex = nextIndexes.TryGetValue(HostNode.NodeId , out int i) ? i : nextIndexes[Guid.Empty];
        using HttpResponseMessage response = await client.PutAsJsonAsync(
                $"{Destination.TrimEnd('/')}/append-entries" +
                $"?requestId={requestId}" +
                $"&requesterId={requesterId}" +
                $"&receiverId={ReceiverId}" +
                $"&requesterTerm={HostNode.CurrentTerm}" +
                $"&previousLogIndex={nextIndex - 1}" +
                $"&previousLogTerm={logEntries[nextIndex - 1].Term}" +
                $"&leaderCommit={commitIndex}" ,
                (IReadOnlyList<LogEntry>)[.. logEntries.Skip(nextIndex)]
            );
        response.EnsureSuccessStatusCode();

        AppendEntriesReply? reply = await response.Content.ReadFromJsonAsync<AppendEntriesReply>();
        if (reply is null)
        {
            Trace.WriteLine("Null reply");
            return;
        }
        Console.WriteLine($"Response for append entries is: RequestId = {reply.RequestId}, AppendSuccess = {reply.AppendSuccess}");
        await HostNode.AppendEntriesReplyChannel.Writer.WriteAsync(reply);
    }
}
