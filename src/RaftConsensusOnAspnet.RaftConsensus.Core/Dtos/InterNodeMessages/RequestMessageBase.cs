namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record RequestMessageBase : MessageBase
{
    public Guid RequesterId { get; set; }
    public Guid ReceiverId { get; set; }
    public int RequesterTerm { get; set; }
}
