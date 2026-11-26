using System.Diagnostics;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public record RaftNodeTraces
{
    public IEnumerable<TraceListener>? StandardTraceListeners { get; set; }
    public IEnumerable<TraceListener>? DebugTraceListeners { get; set; }
    public Dictionary<Guid , int>? NodeIdToDebugPos { get; set; }
}
