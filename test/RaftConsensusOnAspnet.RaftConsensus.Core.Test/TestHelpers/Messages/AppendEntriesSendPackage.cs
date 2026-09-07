using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

public record AppendEntriesSendPackage : MessagePackageBase
{
    public Guid LeaderId;
    public int LeaderCommit;
    public int PreviousLogIndex;
    public int PreviousLogTerm;
    public required IReadOnlyList<LogEntry> Entries;


    /// <inheritdoc />
    public virtual bool Equals(AppendEntriesSendPackage? other)
    {
        if (other is null || this.Entries.Count != other.Entries.Count)
            return false;

        bool identical = true;
        identical &= LeaderId == other.LeaderId;
        identical &= LeaderCommit == other.LeaderCommit;
        identical &= PreviousLogIndex == other.PreviousLogIndex;
        identical &= PreviousLogTerm == other.PreviousLogTerm;

        for (int i = 0; i < Entries.Count; i++)
            identical &= Entries[i].MemberWiseEqualityCheck(other.Entries[i]);

        return identical;
    }
}
