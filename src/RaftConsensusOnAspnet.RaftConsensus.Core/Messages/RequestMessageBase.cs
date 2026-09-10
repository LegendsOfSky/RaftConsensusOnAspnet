namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record RequestMessageBase : MessageBase
{
    public Guid RequesterId;
    public Guid ReceiverId;
    public int RequesterTerm;
}
