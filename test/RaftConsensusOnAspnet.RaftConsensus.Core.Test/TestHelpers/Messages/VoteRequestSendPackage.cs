namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

public record VoteRequestSendPackage : MessagePackageBase
{
    public int LastLogIndex;
    public int LastLogTerm;
}
