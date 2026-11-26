namespace RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;

public record MessageBase
{
    public Guid RequestId { get; set; }
}
