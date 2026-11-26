namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record MessageBase
{
    public Guid RequestId { get; set; }
}
