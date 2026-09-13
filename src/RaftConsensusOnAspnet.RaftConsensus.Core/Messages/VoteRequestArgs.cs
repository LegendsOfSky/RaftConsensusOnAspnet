namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record VoteRequestArgs : RequestMessageBase
{
    public int RequesterLastLogTerm { get; set; }
    public int RequesterLastLogIndex { get; set; }
}
