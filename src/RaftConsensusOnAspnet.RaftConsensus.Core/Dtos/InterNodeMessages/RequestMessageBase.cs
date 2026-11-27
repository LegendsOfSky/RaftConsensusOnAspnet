using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record RequestMessageBase : MessageBase
{
    public Guid RequesterId { get; set; }
    public Guid ReceiverId { get; set; }
    public int RequesterTerm { get; set; }
}
