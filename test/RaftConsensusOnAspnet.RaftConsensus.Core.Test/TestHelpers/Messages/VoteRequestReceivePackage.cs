namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

public record VoteRequestReceivePackage : MessagePackageBase
{
    public bool Granted;
}
