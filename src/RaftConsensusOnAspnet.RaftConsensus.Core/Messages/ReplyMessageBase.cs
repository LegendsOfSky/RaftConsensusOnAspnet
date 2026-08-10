namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record ReplyMessageBase
{
    public Guid ReplierId;
    public Guid ReceiverId;
    public int ReplierTerm;
}
