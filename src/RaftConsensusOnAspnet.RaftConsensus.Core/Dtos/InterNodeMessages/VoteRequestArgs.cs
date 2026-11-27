using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record VoteRequestArgs : RequestMessageBase
{
    public int RequesterLastLogTerm { get; set; }
    public int RequesterLastLogIndex { get; set; }
}
