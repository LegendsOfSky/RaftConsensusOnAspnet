namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class LogEntry
{
    public int Term { get; internal set; }
    public string? Key { get; internal set; }
    public LogEntryOperation Operation { get; internal set; }

    protected static readonly Dictionary<string , Func<string , LogEntry>> s_deserializingFunctions = new Dictionary<string , Func<string , LogEntry>>();
    protected Guid guid;

    private const string LogType = "NonValue";


    public LogEntry() { }

    public LogEntry(int term , LogEntryOperation operation , string? key)
    {
        Term = term;
        Key = key;

        if (operation != LogEntryOperation.None && operation != LogEntryOperation.Delete)
            throw new InvalidOperationException();
        Operation = operation;
    }


    public bool MemberWiseEqualityCheck(object? obj)
    {
        if (obj is not LogEntry other)
            return false;

        bool identical = true;
        identical &= Term      == other.Term;
        identical &= Operation == other.Operation;
        return identical;
    }

    public virtual object? GetValue() => throw new NotSupportedException();

    public virtual string Serialize() => $"{LogType} {guid} {Term} {Operation} {Key}";

    public static LogEntry Deserialize(string raw)
    {
        string[] fragments = raw.Split(' ');
        if (fragments[0] != LogType)
        {
            if (!s_deserializingFunctions.TryGetValue(fragments[0], out Func<string, LogEntry>? deserializeFunction))
                throw new FormatException();
            return deserializeFunction.Invoke(raw);
        }

        if (!Guid.TryParse(fragments[1] , out Guid guid))
            throw new FormatException();
        if (!int.TryParse(fragments[2] , out int term) || !Enum.TryParse(fragments[3] , out LogEntryOperation operation))
            throw new FormatException();
        return new LogEntry(term , operation , fragments[3]) { guid = guid };
    }

    /// <inheritdoc />
    public override string ToString() => $"Term {Term}: None";

    public override bool Equals(object? obj)
    {
        if (obj is not LogEntry comparingEntry)
            return false;

        return guid      == comparingEntry.guid
            && Term      == comparingEntry.Term
            && Key       == comparingEntry.Key
            && Operation == comparingEntry.Operation;
    }
}
