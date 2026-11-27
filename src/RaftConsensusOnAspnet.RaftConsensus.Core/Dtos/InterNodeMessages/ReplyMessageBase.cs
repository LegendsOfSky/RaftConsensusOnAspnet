using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record ReplyMessageBase : MessageBase
{
    public Guid ReplierId { get; set; }
    public Guid ReceiverId { get; set; }
    public int ReplierTerm { get; set; }
}
