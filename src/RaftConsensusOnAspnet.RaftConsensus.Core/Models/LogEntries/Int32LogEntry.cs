using System.Runtime.CompilerServices;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class Int32LogEntry : LogEntry
{
    public int? Value;

    private const string LogType = "Int32";


    static Int32LogEntry()
    {
        s_parsingFunctions[LogType] = Parse;
    }

    public Int32LogEntry(int term , LogEntryOperation operation , string key , int? value)
    {
        Term = term;
        (Operation , Key , Value) = (operation , key , value);
    }


    public new static LogEntry Parse(
        Guid guidIn , int term , LogEntryOperation operation , string _ , string? key , string? serializedValue)
        => key is null
            ? throw new ArgumentNullException(nameof(key))
            : operation is LogEntryOperation.None or LogEntryOperation.Delete
                ? new Int32LogEntry(term , operation , key , null) { Guid = guidIn }
                : serializedValue is null
                    ? throw new ArgumentNullException(nameof(serializedValue))
                    : new Int32LogEntry(term , operation , key , int.Parse(serializedValue));

    /// <inheritdoc />
    public override object? GetValue() => Value;

    /// <inheritdoc />
    public override string GetLogType() => LogType;

    /// <inheritdoc />
    public override string? SerializeValue() => Value is null ? null : Value.ToString();

    /// <inheritdoc />
    public override string ToString() => Operation == LogEntryOperation.None
        ? base.ToString()
        : $"Term {Term}: <{Key}: {(Value is null ? "Null" : Value)}>";

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        if (obj is not Int32LogEntry comparingEntry)
            return false;

        return Guid      == comparingEntry.Guid
            && Term      == comparingEntry.Term
            && Key       == comparingEntry.Key
            && Operation == comparingEntry.Operation
            && Value     == comparingEntry.Value;
    }
}
