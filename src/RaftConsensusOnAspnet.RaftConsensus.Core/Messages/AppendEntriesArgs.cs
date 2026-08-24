using RaftConsensusOnAspnet.RaftConsensus.Core.Models;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntres;

namespace RaftConsensusOnAspnet.RaftConsensus.Core.Messages;

public record AppendEntriesArgs : RequestMessageBase
{
    public int PreviousLogIndex;
    public int PreviousLogTerm;
    public int LeaderCommit;
    public IReadOnlyList<LogEntry> Entries;
}
