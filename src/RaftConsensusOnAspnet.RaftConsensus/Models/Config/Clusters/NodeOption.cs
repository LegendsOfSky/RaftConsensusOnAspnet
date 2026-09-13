namespace RaftConsensusOnAspnet.RaftConsensus.Models.Config.Clusters;

public record NodeOption
{
    public string Name { get; set; } = "";
    public Guid NodeId { get; set; } = Guid.Empty;
    public string Ip { get; set; } = "";
}
