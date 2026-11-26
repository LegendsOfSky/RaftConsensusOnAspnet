namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public record RaftNodeOptions
{
    public Guid? NodeId { get; set; }
    public int ElectionTimeOutInterval { get; set; }
    public int HeartBeatInterval { get; set; }
    public int NodeCount { get; set; }
    public string DatabaseDirectory { get; set; } = "data";
    public bool RemoveExistData { get; set; } = false;
}
