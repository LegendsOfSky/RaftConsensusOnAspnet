namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntres;

public class LogEntry
{
    public int Term { get; internal set; }
    public string? Key { get; internal set; }
    public LogEntryOperation Operation { get; internal set; }


    public LogEntry() { }

    public LogEntry(int term , LogEntryOperation operation , string? key)
    {
        Term = term;
        Key = key;

        if (operation != LogEntryOperation.None && operation != LogEntryOperation.Delete)
            throw new InvalidOperationException();
        Operation = operation;
    }


    public virtual object? GetValue() => throw new NotSupportedException();

    public bool MemberWiseEqualityCheck(object? obj)
    {
        if (obj is not LogEntry other)
            return false;

        bool identical = true;
        identical &= Term      == other.Term;
        identical &= Operation == other.Operation;
        return identical;
    }

    /// <inheritdoc />
    public override string ToString() => $"Term {Term}: None";
}
