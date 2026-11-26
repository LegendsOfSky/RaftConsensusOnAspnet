using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.DTOs.InterNodeMessages;

public record AppendEntriesArgs : RequestMessageBase
{
    public int PreviousLogIndex;
    public int PreviousLogTerm;
    public int LeaderCommit;
    public required IReadOnlyList<LogEntry> Entries;
}
