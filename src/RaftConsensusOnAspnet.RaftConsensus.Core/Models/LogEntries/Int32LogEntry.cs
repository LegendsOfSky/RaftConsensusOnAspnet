namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class Int32LogEntry : LogEntry
{
    public int? Value;

    private const string LogType = "Int32";


    static Int32LogEntry()
    {
        s_deserializingFunctions[LogType] = Deserialize;
    }

    public Int32LogEntry(int term , LogEntryOperation operation , string key , int? value)
    {
        Term = term;
        (Operation , Key , Value) = (operation , key , value);
    }


    public new static LogEntry Deserialize(string raw)
    {
        string[] fragments = raw.Split(' ');
        if (!Guid.TryParse(fragments[1] , out Guid guid))
            throw new FormatException();
        if (!int.TryParse(fragments[2] , out int term) || !Enum.TryParse(fragments[3] , out LogEntryOperation operation))
            throw new FormatException();
        (int? value , string rawValue) = (null , fragments[5]);
        if (rawValue != "Null")
        {
            if (!int.TryParse(rawValue , out int parseResult))
                throw new FormatException();
            value = parseResult;
        }
        return new Int32LogEntry(term , operation , fragments[4] , value) { guid = guid };
    }

    public override object? GetValue() => Value;

    public override string Serialize() => $"{LogType} {guid} {Term} {Operation} {Key} {(Value is null ? "Null" : Value)}";

    public override string ToString() => Operation == LogEntryOperation.None
        ? base.ToString()
        : $"Term {Term}: <{Key}: {(Value is null ? "Null" : Value)}>";

    public override bool Equals(object? obj)
    {
        if (obj is not Int32LogEntry comparingEntry)
            return false;

        return guid      == comparingEntry.guid
            && Term      == comparingEntry.Term
            && Key       == comparingEntry.Key
            && Operation == comparingEntry.Operation
            && Value     == comparingEntry.Value;
    }
}
