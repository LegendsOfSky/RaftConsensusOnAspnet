namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record VoteRequestReply : ReplyMessageBase
{
    public int TermOfRequest { get; set; }
    public bool VoteGranted { get; set; }
}
