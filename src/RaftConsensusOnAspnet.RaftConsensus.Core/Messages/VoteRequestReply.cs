namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record VoteRequestReply : ReplyMessageBase
{
    public bool VoteGranted;
}
