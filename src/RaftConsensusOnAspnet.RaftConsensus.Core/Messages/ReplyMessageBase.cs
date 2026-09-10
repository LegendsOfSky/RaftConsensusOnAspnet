namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record ReplyMessageBase : MessageBase
{
    public Guid ReplierId;
    public Guid ReceiverId;
    public int ReplierTerm;
}
