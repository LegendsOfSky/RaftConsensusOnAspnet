using System.Collections;

namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntres;

public class LogEntryList : IList<LogEntry> , IReadOnlyList<LogEntry>
{
    public LogEntry this[int index]
    {
        get => ReadFromDisk()[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => ReadFromDisk().Count;

    private readonly string filePath;


    public LogEntryList(Guid nodeIdIn)
    {
        filePath = $"{nodeIdIn}.LogEntries";
    }


    public void AddRange(IEnumerable<LogEntry> newEntries)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        persistentEntries.AddRange(newEntries);
        WriteToDisk(persistentEntries);
    }

    public void RemoveRange(int start , int count)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        persistentEntries.RemoveRange(start , count);
        WriteToDisk(persistentEntries);
    }

    private List<LogEntry> ReadFromDisk()
    {
        if (!File.Exists(filePath))
            return [];

        string[] rawLogEntries = File.ReadAllLines(filePath);
        return rawLogEntries.Select(LogEntry.Deserialize).ToList();
    }

    private void WriteToDisk(IEnumerable<LogEntry> entriesToSerialize)
    {
        string[] rawLogEntries = entriesToSerialize.Select(entry => entry.Serialize()).ToArray();
        File.WriteAllLines(filePath , rawLogEntries);
    }


    #region Interface implementations
    public void Add(LogEntry entry)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        persistentEntries.Add(entry);
        WriteToDisk(persistentEntries);
    }

    public void Insert(int index , LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        persistentEntries.Insert(index , item);
        WriteToDisk(persistentEntries);
    }

    public bool Remove(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        bool success = persistentEntries.Remove(item);
        WriteToDisk(persistentEntries);
        return success;
    }

    public void RemoveAt(int index)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        persistentEntries.RemoveAt(index);
        WriteToDisk(persistentEntries);
    }

    public void Clear()
    {
        WriteToDisk([]);
    }

    public int IndexOf(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        return persistentEntries.IndexOf(item);
    }

    public bool Contains(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDisk();
        return persistentEntries.Contains(item);
    }

    public void CopyTo(LogEntry[] array , int arrayIndex)
    {
        ReadFromDisk().CopyTo(array , arrayIndex);
    }

    public IEnumerator<LogEntry> GetEnumerator() => ReadFromDisk().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    #endregion
}
