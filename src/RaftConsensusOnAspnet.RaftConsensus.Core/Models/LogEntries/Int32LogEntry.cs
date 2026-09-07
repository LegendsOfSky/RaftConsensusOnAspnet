namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class Int32LogEntry : LogEntry
{
    public int? Value { get; init; }

    public new const string LogType = "Int32";


    static Int32LogEntry()
    {
        s_parsingFunctions[LogType] = Parse;
    }

    public Int32LogEntry(int term , LogEntryOperation operation , string key , int? value)
    {
        Term = term;
        Guid = Guid.NewGuid();
        (Operation , Key , Value) = (operation , key , value);
    }

    public Int32LogEntry(Guid guid , int term , LogEntryOperation operation , string key , int? value)
    {
        (Guid , Term , Operation) = (guid , term , operation);
        (Key , Value) = (key , value);
    }


    public new static LogEntry Parse(
        Guid guidIn , int term , LogEntryOperation operation , string _ , string? key , string? serializedValue)
        => key is null
            ? throw new ArgumentNullException(nameof(key))
            : operation is LogEntryOperation.None or LogEntryOperation.Delete
                ? new Int32LogEntry(guidIn , term , operation , key , null)
                : serializedValue is null
                    ? throw new ArgumentNullException(nameof(serializedValue))
                    : new Int32LogEntry(guidIn , term , operation , key , int.Parse(serializedValue));

    /// <inheritdoc />
    public override object? GetValue() => Value;

    /// <inheritdoc />
    public override string GetLogType() => LogType;

    /// <inheritdoc />
    public override string? SerializeValue() => Value?.ToString();

    /// <inheritdoc />
    public override string ToString() => Operation == LogEntryOperation.None
        ? base.ToString()
        : $"Term {Term}: <{Key}: {(Value is null ? "Null" : Value)}>";

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is Int32LogEntry comparingEntry
            && Guid      == comparingEntry.Guid
            && Term      == comparingEntry.Term
            && Key       == comparingEntry.Key
            && Operation == comparingEntry.Operation
            && Value     == comparingEntry.Value;
    }

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Guid);
}
