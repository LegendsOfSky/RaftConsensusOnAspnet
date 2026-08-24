namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntres;

public class Int32LogEntry : LogEntry
{
    public int? Value;


    public Int32LogEntry(int term , LogEntryOperation operation , string key , int? value)
    {
        Term = term;
        (Operation , Key , Value) = (operation , key , value);
    }


    public override object? GetValue() => Value;

    public override string ToString() => Operation == LogEntryOperation.None
        ? base.ToString()
        : $"Term {Term}: <{Key}: {(Value is null ? "Null" : Value)}>";
}
