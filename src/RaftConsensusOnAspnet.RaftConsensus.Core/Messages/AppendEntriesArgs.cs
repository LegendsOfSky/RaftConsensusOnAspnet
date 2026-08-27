using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record AppendEntriesArgs : RequestMessageBase
{
    public int PreviousLogIndex;
    public int PreviousLogTerm;
    public int LeaderCommit;
    public required IReadOnlyList<LogEntry> Entries;
}
