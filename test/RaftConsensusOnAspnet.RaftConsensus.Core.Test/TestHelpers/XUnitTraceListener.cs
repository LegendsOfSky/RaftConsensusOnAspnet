using System.Diagnostics;
using Xunit.Abstractions;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Test.TestHelpers;

public sealed class XUnitTraceListener : TraceListener
{
    private readonly ITestOutputHelper testOutput;
    private readonly Lock gate = new Lock();


    public XUnitTraceListener(ITestOutputHelper output)
    {
        testOutput = output ?? throw new ArgumentNullException(nameof(output));
        Name = "xunit";
    }


    public override void TraceEvent(TraceEventCache? cache , string source , TraceEventType type , int id , string? message)
    {
        if (Filter is not null && !Filter.ShouldTrace(cache , source , type , id , message , null , null , null))
            return;
        WriteAligned(source , type , id , message);
    }

    public override void TraceEvent(TraceEventCache? cache , string source , TraceEventType type , int id , string? format , params object?[]? args)
    {
        string msg = format is null
            ? args is null
                ? ""
                : args.Aggregate("" , (previous , arg) => previous + arg)
            : args is { Length: > 0 }
                ? string.Format(format , args)
                : format;
        TraceEvent(cache , source , type , id , msg);
    }

    public override void TraceData(TraceEventCache? cache , string source , TraceEventType type , int id , object? data)
        => TraceEvent(cache , source , type , id , data?.ToString());

    public override void TraceData(TraceEventCache? cache , string source , TraceEventType type , int id , params object?[]? data)
        => TraceEvent(
                    cache , source , type , id ,
                    data is null ? "" : string.Join(" | " , data.Select(x => x?.ToString()))
                );

    public override void TraceTransfer(TraceEventCache? cache , string source , int id , string? message , Guid relatedActivityId)
        => TraceEvent(
                cache , source , TraceEventType.Transfer , id ,
                $"{message} current={Trace.CorrelationManager.ActivityId:D} related={relatedActivityId:D}"
            );

    private void WriteAligned(string source , TraceEventType type , int id , string? message)
    {
        string time = DateTime.Now.ToString("HH:mm:ss.fffffff");
        string line = $"{source} {type,-11} {id,4} {time} {message}";

        lock (gate)
        {
            try { testOutput.WriteLine(line); }
            catch (InvalidOperationException)
            {
                // test already finished; background Raft thread still tracing
            }
        }
    }

    public override void Write(string? message)
    {
        lock (gate)
        {
            try { testOutput.WriteLine(message ?? ""); }
            catch (InvalidOperationException) { }
        }
    }

    public override void WriteLine(string? message) => Write(message);
}
