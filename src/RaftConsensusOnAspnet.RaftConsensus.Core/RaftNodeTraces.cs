using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

[ExcludeFromCodeCoverage]
public record RaftNodeTraces
{
    public IEnumerable<TraceListener>? StandardTraceListeners { get; set; }
    public IEnumerable<TraceListener>? DebugTraceListeners { get; set; }
    public Dictionary<Guid , int>? NodeIdToDebugPos { get; set; }
}
