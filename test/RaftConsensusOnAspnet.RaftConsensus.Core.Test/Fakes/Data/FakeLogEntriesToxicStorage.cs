using System.Collections;
using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.Fakes.Data;

public class FakeLogEntriesToxicStorage : ToxicStorageBase , ILogEntriesPersistentStorage
{
    public LogEntry this[int index]
    {
        get
        {
            ThrowAndTraceIfCorrupted();
            WaitForProcessingDelay();
            return logEntries[index];
        }
        set => throw new NotSupportedException();
    }
    public int Count
    {
        get
        {
            ThrowAndTraceIfCorrupted();
            WaitForProcessingDelay();
            return logEntries.Count;
        }
    }
    /// <inheritdoc/>
    public bool IsReadOnly => false;

    private readonly List<LogEntry> logEntries;


    public FakeLogEntriesToxicStorage(
        Guid nodeId , IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null)
    {
        logEntries = [];

        /* Setup logging */
        standardTrace = new TraceSource($"{"LogEntry",-12}.{nodeId}" , SourceLevels.All);  // <--+-< standard trace
        standardTrace.Listeners.Clear();                                                   //    |
        foreach (TraceListener listener in standardTraceListenersIn ?? [])                 //    |
            standardTrace.Listeners.Add(listener);  // <-----------------------------------------+
        debugTrace = new TraceSource($"{"LogEntry",-12}.{nodeId}" , SourceLevels.All);  // <--+-< debug trace
        debugTrace.Listeners.Clear();                                                   //    |
        foreach (TraceListener listener in debugTraceListenersIn ?? [])                 //    |
            debugTrace.Listeners.Add(listener);  // <-----------------------------------------+
    }


    #region Interface Implementations
    /// <inheritdoc/>
    public void Add(LogEntry item) => AddRange([item]);

    public void AddRange(IReadOnlyList<LogEntry> newEntries) => TryOverrideEntriesStartFromAndEraseFutureEntries(newEntries , logEntries.Count);

    /// <inheritdoc/>
    public void Clear() => TryOverrideEntriesStartFromAndEraseFutureEntries([] , 0);

    /// <inheritdoc/>
    public bool Contains(LogEntry item)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        return logEntries.Contains(item);
    }

    /// <inheritdoc/>
    public void CopyTo(LogEntry[] array , int arrayIndex)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        logEntries.CopyTo(array , arrayIndex);
    }

    /// <inheritdoc/>
    public IEnumerator<LogEntry> GetEnumerator()
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        return logEntries.GetEnumerator();
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator()
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        return logEntries.GetEnumerator();
    }

    /// <inheritdoc/>
    public int IndexOf(LogEntry item)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        return logEntries.IndexOf(item);
    }

    /// <inheritdoc/>
    public void Insert(int index , LogEntry item)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        ThrowAndTraceIfUncommittable();
        logEntries.Insert(index , item);
    }

    public bool TryOverrideEntriesStartFromAndEraseFutureEntries(IReadOnlyList<LogEntry> entriesToAppend , int startIndex)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        ThrowAndTraceIfUncommittable();
        if (startIndex < logEntries.Count)
            RemoveRange(startIndex , logEntries.Count - startIndex);
        logEntries.AddRange(entriesToAppend);
        return true;
    }

    /// <inheritdoc/>
    public bool Remove(LogEntry item)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        ThrowAndTraceIfUncommittable();
        return !IsReadOnly && logEntries.Remove(item);
    }

    /// <inheritdoc/>
    public void RemoveAt(int index) => RemoveRange(index , 1);

    public void RemoveExistingData() => RemoveRange(0 , logEntries.Count);

    public void RemoveRange(int start , int count)
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        ThrowAndTraceIfUncommittable();
        logEntries.RemoveRange(start , count);
    }
    #endregion
}
