namespace RaftConsensusOnAspnet.RaftConsensus.Core;

public class LogEntry
{
    public int Term { get; internal set; }
    public LogEntryOperation Operation { get; internal set; }
    public string? Key { get; internal set; }
    public object? Value { get; internal set; }


    public LogEntry() { }

    public LogEntry(int term , LogEntryOperation operation , string? key , object? value)
    {
        Term = term;
        Operation = operation;
        (Key , Value) = (key , value);
    }


    public bool MemberWiseEqualityCheck(object? obj)
    {
        if (obj is not LogEntry other)
            return false;

        bool identical = true;
        identical &= Term      == other.Term;
        identical &= Operation == other.Operation;
        identical &= Key       == other.Key;
        identical &= Equals(Value , other.Value);
        return identical;
    }

    /// <inheritdoc />
    public override string ToString() => Operation == LogEntryOperation.None
        ? $"Term {Term}: None"
        : $"Term {Term}: {Operation} <{Key}: {Value ?? "N/A"}>";
}
