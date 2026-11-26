using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;

public interface ILogEntriesPersistentStorage : IList<LogEntry> , IReadOnlyList<LogEntry>
{
    public new LogEntry this[int index] { get; }
    public new int Count { get; }


    public void AddRange(IReadOnlyList<LogEntry> newEntries);
    public bool TryOverrideEntriesStartFromAndEraseFutureEntries(IReadOnlyList<LogEntry> entriesToAppend , int startIndex);
    public void RemoveExistingData();
    public void RemoveRange(int start , int count);
}
