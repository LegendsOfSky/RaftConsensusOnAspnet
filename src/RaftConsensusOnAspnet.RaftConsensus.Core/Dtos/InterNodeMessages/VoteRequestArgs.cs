namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record VoteRequestArgs : RequestMessageBase
{
    public int RequesterLastLogTerm { get; set; }
    public int RequesterLastLogIndex { get; set; }
}
