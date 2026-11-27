using System.Diagnostics.CodeAnalysis;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Dtos.InterNodeMessages;

[ExcludeFromCodeCoverage]
public record AppendEntriesArgs : RequestMessageBase
{
    public int PreviousLogIndex;
    public int PreviousLogTerm;
    public int LeaderCommit;
    public required IReadOnlyList<LogEntry> Entries;
}
