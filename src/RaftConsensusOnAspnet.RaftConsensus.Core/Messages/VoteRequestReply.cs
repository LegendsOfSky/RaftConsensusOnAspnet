namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record VoteRequestReply : ReplyMessageBase
{
    public int TermOfRequest;
    public bool VoteGranted;
}
