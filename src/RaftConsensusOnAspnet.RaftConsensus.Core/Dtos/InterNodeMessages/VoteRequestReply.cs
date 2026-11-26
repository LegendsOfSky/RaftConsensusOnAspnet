namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record VoteRequestReply : ReplyMessageBase
{
    public int TermOfRequest { get; set; }
    public bool VoteGranted { get; set; }
}
