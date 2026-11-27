using System.Diagnostics;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.Fakes.Data;

public class FakeRaftNodeMetadataToxicStorage : ToxicStorageBase , IRaftNodeMetadataPersistentStorage
{
    public int CurrentTerm
    {
        get
        {
            ThrowAndTraceIfCorrupted();
            WaitForProcessingDelay();
            return field;
        }
        set
        {
            ThrowAndTraceIfCorrupted();
            WaitForProcessingDelay();
            ThrowAndTraceIfUncommittable();
            field = value;
        }
    }
    public (Guid VoteFor , int TermOfVote) VoteInfo
    {
        get
        {
            ThrowAndTraceIfCorrupted();
            return field;
        }
        set
        {
            ThrowAndTraceIfCorrupted();
            WaitForProcessingDelay();
            ThrowAndTraceIfUncommittable();
            field = value;
        }
    }


    public FakeRaftNodeMetadataToxicStorage(
        Guid nodeId , IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null)
    {
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


    public void RemoveExistingData()
    {
        ThrowAndTraceIfCorrupted();
        WaitForProcessingDelay();
        ThrowAndTraceIfUncommittable();

        CurrentTerm = 0;
        VoteInfo = (Guid.Empty , 0);
    }
}
