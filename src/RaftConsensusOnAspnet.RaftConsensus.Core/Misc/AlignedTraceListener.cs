using System.Diagnostics;
using System.Text;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Misc;

public sealed class AlignedTraceListener : TraceListener
{
    private readonly TextWriter textWriter;
    private readonly Lock gate = new Lock();
    private readonly bool ownsWriter;


    public AlignedTraceListener(string path) : this(CreateWriter(path) , true)
    {
        Name = path;
    }

    public AlignedTraceListener(TextWriter writer , bool ownsWriterIn = false)
    {
        textWriter = writer ?? throw new ArgumentNullException(nameof(writer));
        ownsWriter = ownsWriterIn;
    }


    private static StreamWriter CreateWriter(string path)
    {
        string? dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        return new StreamWriter(new FileStream(path , FileMode.Append , FileAccess.Write , FileShare.Read) , Encoding.UTF8)
        {
            AutoFlush = true ,
        };
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
    {
        Guid current = Trace.CorrelationManager.ActivityId;
        TraceEvent(
                cache , source , TraceEventType.Transfer , id ,
                $"{message} current={current:D} related={relatedActivityId:D}"
            );
    }

    private void WriteAligned(string source , TraceEventType type , int id , string? message)
    {
        string time = DateTime.Now.ToString("HH:mm:ss.fffffff");
        string line = $"{source} {type,-11} {id,4} {time} {message}";

        lock (gate)
            textWriter.WriteLine(line);
    }

    public override void Write(string? message)
    {
        lock (gate) textWriter.Write(message);
    }

    public override void WriteLine(string? message)
    {
        lock (gate) textWriter.WriteLine(message);
    }

    public override void Flush()
    {
        lock (gate) textWriter.Flush();
    }

    public override void Close()
    {
        lock (gate)
        {
            textWriter.Flush();
            if (ownsWriter)
                textWriter.Dispose();
        }
        base.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();
        base.Dispose(disposing);
    }
}