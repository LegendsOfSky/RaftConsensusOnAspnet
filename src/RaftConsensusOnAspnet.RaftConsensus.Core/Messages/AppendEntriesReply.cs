namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record AppendEntriesReply : ReplyMessageBase
{
    public bool AppendSuccess { get; set; }
    public int MatchIndex { get; set; }
}
