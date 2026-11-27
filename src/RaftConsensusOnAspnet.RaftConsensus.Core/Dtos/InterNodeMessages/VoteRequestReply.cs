using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record VoteRequestReply : ReplyMessageBase
{
    public int TermOfRequest { get; set; }
    public bool VoteGranted { get; set; }
}
