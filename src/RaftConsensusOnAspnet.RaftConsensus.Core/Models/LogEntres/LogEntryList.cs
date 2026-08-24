using System.Collections;

namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntres;

public class LogEntryList : IList<LogEntry> , IReadOnlyList<LogEntry>
{
    public LogEntry this[int index]
    {
        get => entries[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => entries.Count;

    private string filePath;
    private Guid nodeId;
    private List<LogEntry> entries = [];


    public LogEntryList(Guid nodeIdIn)
    {
        nodeId = nodeIdIn;
        filePath = $"{nodeId}.LogEntries";
    }


    public void AddRange(IEnumerable<LogEntry> newEntries)
    {
        entries.AddRange(newEntries);
    }

    public void RemoveRange(int start , int count)
    {
        entries.RemoveRange(start , count);
    }

    #region Interface implementations
    public void Add(LogEntry entry)
    {
        entries.Add(entry);
    }

    public void Insert(int index , LogEntry item)
    {
        entries.Insert(index , item);
    }

    public bool Remove(LogEntry item)
    {
        return entries.Remove(item);
    }

    public void RemoveAt(int index)
    {
        entries.RemoveAt(index);
    }

    public void Clear()
    {
        entries = [];
    }

    public int IndexOf(LogEntry item)
    {
        return entries.IndexOf(item);
    }

    public bool Contains(LogEntry item)
    {
        return entries.Contains(item);
    }

    public void CopyTo(LogEntry[] array , int arrayIndex)
    {
        entries.CopyTo(array , arrayIndex);
    }

    public IEnumerator<LogEntry> GetEnumerator() => entries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    #endregion
}
