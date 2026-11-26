namespace RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;

public record VoteRequestArgs : RequestMessageBase
{
    public int RequesterLastLogTerm { get; set; }
    public int RequesterLastLogIndex { get; set; }
}
