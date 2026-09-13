namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record ReplyMessageBase : MessageBase
{
    public Guid ReplierId { get; set; }
    public Guid ReceiverId { get; set; }
    public int ReplierTerm { get; set; }
}
