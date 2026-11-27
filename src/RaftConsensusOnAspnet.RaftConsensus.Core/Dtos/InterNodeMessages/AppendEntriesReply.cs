using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record AppendEntriesReply : ReplyMessageBase
{
    public bool AppendSuccess { get; set; }
    public int MatchIndex { get; set; }
}
