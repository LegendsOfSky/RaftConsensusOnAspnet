using System.ComponentModel.DataAnnotations;
using System.Diagnostics;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.Fakes.Data;

public abstract class ToxicStorageBase
{
    /// <summary> Defines processing delay as latency +/- jitter (unit: milliseconds). </summary>
    public (int Latency , int Jitter) ProcessingDelay { get; set; } = (0 , 0);
    [Range(0D , 1D , ErrorMessage = "Value for {0} must be between {1} and {2}.")]
    public double CommitSuccessRate { get; set; } = 1D;
    [Range(0D , 1D , ErrorMessage = "Value for {0} must be between {1} and {2}.")]
    public double CorruptedRate { get; set; } = 0D;

    protected TraceSource? debugTrace;
    protected TraceSource? standardTrace;


    protected void ThrowAndTraceIfCorrupted()
    {
        const string TraceMessage = "Persistent storage for log entries is corrupted.";

        double hitScore = Random.Shared.NextDouble();
        if (CorruptedRate != 0D && hitScore < CorruptedRate)
        {
            standardTrace?.TraceEvent(TraceEventType.Critical , 0 , TraceMessage);
            debugTrace?.TraceEvent(TraceEventType.Critical , 0 , TraceMessage);
        }
    }

    protected void ThrowAndTraceIfUncommittable()
    {
        const string TraceMessage = "Cannot commit to persistent storage for log entries.";

        double hitScore = Random.Shared.NextDouble();
        if (CommitSuccessRate < 1D && hitScore < 1D - CommitSuccessRate)
        {
            standardTrace?.TraceEvent(TraceEventType.Critical , 0 , TraceMessage);
            debugTrace?.TraceEvent(TraceEventType.Critical , 0 , TraceMessage);
        }
    }

    protected void WaitForProcessingDelay()
    {
        if (ProcessingDelay == (0 , 0))
            return;

        int minDelay = Math.Max(0 , ProcessingDelay.Latency - ProcessingDelay.Jitter);
        int delay = minDelay + (int)(ProcessingDelay.Jitter * 2 * Random.Shared.NextDouble());
        Thread.Sleep(delay);
    }
}
