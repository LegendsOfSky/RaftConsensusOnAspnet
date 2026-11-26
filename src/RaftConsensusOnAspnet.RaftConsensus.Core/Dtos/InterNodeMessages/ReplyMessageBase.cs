namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record ReplyMessageBase : MessageBase
{
    public Guid ReplierId { get; set; }
    public Guid ReceiverId { get; set; }
    public int ReplierTerm { get; set; }
}
