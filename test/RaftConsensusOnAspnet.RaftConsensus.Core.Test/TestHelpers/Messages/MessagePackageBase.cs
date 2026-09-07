namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers.Messages;

public abstract record MessagePackageBase
{
    public bool MessageDropped;
    public int Delay;
    public int Term;
}
