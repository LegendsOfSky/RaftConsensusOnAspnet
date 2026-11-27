using System.Diagnostics.CodeAnalysis;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;


namespace RaftConsensusOnAspnet.RaftConsensus.Core;

[ExcludeFromCodeCoverage]
public record RaftNodeOptions
{
    public ILogEntriesPersistentStorage? CustomLogEntriesPersistentStorage { get; set; }
    public IRaftNodeMetadataPersistentStorage? CustomRaftNodeMetadataPersistentStorage { get; set; }
    public string DatabaseDirectory { get; set; } = "data";
    public int ElectionTimeOutInterval { get; set; }
    public int HeartBeatInterval { get; set; }
    public int NodeCount { get; set; }
    public Guid? NodeId { get; set; }
    public bool RemoveExistData { get; set; } = false;
}
