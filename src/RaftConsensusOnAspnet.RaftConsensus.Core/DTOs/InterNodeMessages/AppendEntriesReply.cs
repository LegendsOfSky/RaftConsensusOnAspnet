namespace RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;

public record AppendEntriesReply : ReplyMessageBase
{
    public bool AppendSuccess { get; set; }
    public int MatchIndex { get; set; }
}
