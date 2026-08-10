namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record VoteRequestArgs : RequestMessageBase
{
    public int RequesterLastLogTerm;
    public int RequesterLastLogIndex;
}
