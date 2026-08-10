namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class LogEntry
{
    public int Term { get; internal set; }


    /// <inheritdoc />
    public override string ToString()
    {
        return $"Term {Term}: TODO value";
    }
}
