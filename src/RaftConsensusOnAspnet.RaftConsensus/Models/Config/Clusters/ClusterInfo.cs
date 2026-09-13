namespace RaftConsensusOnAspnet.RaftConsensus.Models.Config.Clusters;

internal record ClusterInfo
{
    public List<NodeOption> Nodes { get; set; } = [];
}
