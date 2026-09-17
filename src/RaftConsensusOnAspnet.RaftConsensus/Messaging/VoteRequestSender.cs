using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;


namespace RaftConsensusOnAspnet.RaftConsensus.Messaging;

internal class VoteRequestSender
{
    public const string ClientName = "requestVoteClient";
    public string Destination;
    public Guid ReceiverId;
    public RaftNode HostNode;
    private readonly IHttpClientFactory httpClientFactory;


    public VoteRequestSender(IHttpClientFactory httpClientFactoryIn , RaftNode hostNode , string destination , Guid receiverId)
    {
        httpClientFactory = httpClientFactoryIn;
        HostNode = hostNode;
        Destination = destination;
        ReceiverId = receiverId;
    }


    public async Task SendVoteRequestAsync(Guid requestId , Guid requesterId , int commitIndex , int previousLogTerm)
    {
        HttpClient client = httpClientFactory.CreateClient(ClientName);
        Console.WriteLine($"sending to {ReceiverId}");
        using HttpResponseMessage response = await client.PatchAsync(
                    $"{Destination.TrimEnd('/')}/api/node/vote" +
                    $"?requestId={requestId}" +
                    $"&requesterId={requesterId}" +
                    $"&requesterTerm={HostNode.CurrentTerm}" +
                    $"&requesterLastLogTerm={commitIndex}" +
                    $"&requesterLastLogIndex={previousLogTerm}" ,
                    null
                );
        response.EnsureSuccessStatusCode();

        VoteRequestReply? reply = await response.Content.ReadFromJsonAsync<VoteRequestReply>();
        if (reply is null)
        {
            Trace.WriteLine("Null reply");
            return;
        }
        Console.WriteLine($"Response for vote request is: RequestId = {reply.RequestId}, TermOfRequest = {reply.TermOfRequest}, VoteGranted = {reply.VoteGranted}");
        await HostNode.VoteRequestReplyChannel.Writer.WriteAsync(reply);
    }
}
