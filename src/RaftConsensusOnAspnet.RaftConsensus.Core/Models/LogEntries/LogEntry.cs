using System.Reflection;
using System.Runtime.CompilerServices;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class LogEntry
{
    public int Term { get; internal set; }
    public string? Key { get; internal set; }
    public LogEntryOperation Operation { get; internal set; }

    internal Guid Guid;

    protected static readonly Dictionary<string , Func<Guid , int , LogEntryOperation , string , string? , string? , LogEntry>> s_parsingFunctions = [];

    private const string LogType = "NonValue";


    static LogEntry()
    {
        IEnumerable<Type> subTypes = Assembly.GetExecutingAssembly().GetTypes().Where(t => t.IsSubclassOf(typeof(LogEntry)) && !t.IsAbstract);
        foreach (Type type in subTypes)
            RuntimeHelpers.RunClassConstructor(type.TypeHandle);
    }

    public LogEntry() { }

    public LogEntry(int term , LogEntryOperation operation , string? key)
    {
        Term = term;
        Key = key;

        if (operation != LogEntryOperation.None && operation != LogEntryOperation.Delete)
            throw new InvalidOperationException();
        Operation = operation;
    }


    public static LogEntry Parse(
        Guid guidIn , int term , LogEntryOperation operation , string logType , string? key , string? serializedValue)
    {
        if (logType == LogType)
            return new LogEntry(term , operation , key) { Guid = guidIn };

        if (!s_parsingFunctions.TryGetValue(logType , out Func<Guid , int , LogEntryOperation , string , string? , string? , LogEntry>? parsingFunction))
            throw new FormatException();
        return parsingFunction.Invoke(guidIn , term , operation , logType , key , serializedValue);
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

    public virtual string GetLogType() => LogType;

    public virtual string? SerializeValue() => null;

    /// <inheritdoc />
    public override string ToString() => $"Term {Term}: None";

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        if (obj is not LogEntry comparingEntry)
            return false;

        return Guid      == comparingEntry.Guid
            && Term      == comparingEntry.Term
            && Key       == comparingEntry.Key
            && Operation == comparingEntry.Operation;
    }
}
