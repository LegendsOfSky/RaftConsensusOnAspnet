namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record RequestMessageBase
{
    public Guid RequesterId;
    public Guid ReceiverId;
    public int RequesterTerm;
}
