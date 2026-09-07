namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

public record AppendEntriesReceivePackage : MessagePackageBase
{
    public bool Success;
    public int MatchIndex;
}
