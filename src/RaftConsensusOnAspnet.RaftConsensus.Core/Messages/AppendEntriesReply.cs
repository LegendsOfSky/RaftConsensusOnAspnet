namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record AppendEntriesReply : ReplyMessageBase
{
    public bool AppendSuccess;
    public int MatchIndex;
}
