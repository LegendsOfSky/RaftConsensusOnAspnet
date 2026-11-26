namespace RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;

public record ReplyMessageBase : MessageBase
{
    public Guid ReplierId { get; set; }
    public Guid ReceiverId { get; set; }
    public int ReplierTerm { get; set; }
}
