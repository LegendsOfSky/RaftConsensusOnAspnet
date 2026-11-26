namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

public record AppendEntriesReply : ReplyMessageBase
{
    public bool AppendSuccess { get; set; }
    public int MatchIndex { get; set; }
}
