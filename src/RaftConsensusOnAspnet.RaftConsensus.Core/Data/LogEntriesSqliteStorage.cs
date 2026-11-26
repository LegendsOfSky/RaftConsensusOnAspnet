using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;
using RaftConsensusOnAspnet.RaftConsensus.Core.Miscs;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Data;

public class LogEntriesSqliteStorage : ILogEntriesPersistentStorage
{
    public LogEntry this[int index]
    {
        get => CachedLogEntries[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => CachedLogEntries.Count;
    [NotNull] private IReadOnlyList<LogEntry>? CachedLogEntries
    {
        get => field ??= ReadFromDatabase();
        set => field = value is null ? null : field;
    }

    public const string StateMachineValuesTableName = "StateMachineValues";

    private readonly string dbFilePath;
    private readonly TraceSource debugTrace;
    private readonly TraceSource standardTrace;


    public LogEntriesSqliteStorage(string filePath , Guid nodeId ,
                        IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null)
    {
        dbFilePath = filePath;
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);

        CreateTableForLogEntries(connection);
        CreateTableForStateMachineValues(connection);

        /* Setup logging */
        standardTrace = new TraceSource($"{"LogEntry",-12}.{nodeId}" , SourceLevels.All);  // <--+-< standard trace
        standardTrace.Listeners.Clear();                                                   //    |
        foreach (TraceListener listener in standardTraceListenersIn ?? [])                 //    |
            standardTrace.Listeners.Add(listener);  // <-----------------------------------------+
        debugTrace = new TraceSource($"{"LogEntry",-12}.{nodeId}" , SourceLevels.All);  // <--+-< debug trace
        debugTrace.Listeners.Clear();                                                   //    |
        foreach (TraceListener listener in debugTraceListenersIn ?? [])                 //    |
            debugTrace.Listeners.Add(listener);  // <-----------------------------------------+
    }


    public void AddRange(IReadOnlyList<LogEntry> newEntries) => TryOverrideEntriesStartFromAndEraseFutureEntries(newEntries , Count);

    /// <remarks> <b>REMARKS:</b>
    ///     <list type="bullet">
    ///         <item> Removes all entries after startIndex. </item>
    ///         <item> Invalidates <see cref="CachedLogEntries"/>. </item>
    ///     </list>
    /// </remarks>
    public bool TryOverrideEntriesStartFromAndEraseFutureEntries(IReadOnlyList<LogEntry> entriesToAppend , int startIndex)
    {
        if (Count < startIndex)
        {
            standardTrace.TraceEvent(TraceEventType.Warning , 0 , "Append entries failed because of missing logs");
            return false;
        }

        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            RemoveEntriesInRangeViaTransactionConnection(startIndex , Count - startIndex  , transaction , connection);

            using SqliteCommand addEntry = new SqliteCommand(
                    """
                    INSERT INTO LogEntries (Id , Guid , Term , Operation , LogType , Key , Value)
                    VALUES (
                            $id , $guid , $term , $operation , $logType , $key , $value
                        );
                    """ , connection , transaction
                );
            SqliteParameter varId        = addEntry.Parameters.Add("$id"        , SqliteType.Integer);
            SqliteParameter varGuid      = addEntry.Parameters.Add("$guid"      , SqliteType.Text   );
            SqliteParameter varTerm      = addEntry.Parameters.Add("$term"      , SqliteType.Integer);
            SqliteParameter varOperation = addEntry.Parameters.Add("$operation" , SqliteType.Integer);
            SqliteParameter varLogType   = addEntry.Parameters.Add("$logType"   , SqliteType.Text   );
            SqliteParameter varKey       = addEntry.Parameters.Add("$key"       , SqliteType.Text   );
            SqliteParameter varValue     = addEntry.Parameters.Add("$value"     , SqliteType.Text   );
            int index = startIndex;
            foreach (LogEntry entry in entriesToAppend)
            {
                varId       .Value = index++;
                varGuid     .Value = entry.Guid.ToString();
                varTerm     .Value = entry.Term;
                varOperation.Value = (int)entry.Operation;
                varLogType  .Value = entry.GetLogType();
                varKey      .Value = entry.Key              ?? (object)DBNull.Value;
                varValue    .Value = entry.SerializeValue() ?? (object)DBNull.Value;
                addEntry.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception e)
        {
            standardTrace.TraceEvent(TraceEventType.Error , 0 , $"Cannot insert new log entry (start at {startIndex}) to database. Error: \n{e}");
            transaction.Rollback();
            return false;
        }
        CachedLogEntries = null;
        return true;
    }

    public void RemoveExistingData() => Clear();

    /// <remarks> <b>REMARKS:</b> Invalidates <see cref="CachedLogEntries"/>. </remarks>
    public void RemoveRange(int start , int count)
    {
        if (start >= CachedLogEntries.Count)
            return;

        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            RemoveEntriesInRangeViaTransactionConnection(start , count  , transaction , connection);
            transaction.Commit();
        }
        catch (Exception e)
        {
            standardTrace.TraceEvent(TraceEventType.Error , 0 , $"Cannot remove log entries from {start} to {start + count}(Exclusive). Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }

    private static void CreateTableForStateMachineValues(SqliteConnection connection)
    {
        using SqliteCommand createStateMachineValuesTable = new SqliteCommand(
                $"""
                CREATE TABLE IF NOT EXISTS {StateMachineValuesTableName} (
                    Key   TEXT NOT NULL UNIQUE ,
                    Value TEXT NOT NULL ,
                    Type  TEXT NOT NULL
                );
                """ , connection
            );
        createStateMachineValuesTable.ExecuteNonQuery();
    }

    private static void CreateTableForLogEntries(SqliteConnection connection)
    {
        using SqliteCommand createLogEntriesTable = new SqliteCommand(
                """
                CREATE TABLE IF NOT EXISTS LogEntries (
                    Id        INTEGER NOT NULL UNIQUE ,
                    Guid      TEXT    NOT NULL UNIQUE ,
                    Term      INTEGER NOT NULL ,
                    Operation INTEGER NOT NULL ,
                    LogType   TEXT    NOT NULL ,
                    Key       TEXT ,
                    Value     TEXT
                );
                """ , connection
            );
        createLogEntriesTable.ExecuteNonQuery();
    }

    /// <summary> Removes "<paramref name="count"/>" amount of entries starting from "<paramref name="startIndex"/>" without leaving a gap. </summary>
    private static void RemoveEntriesInRangeViaTransactionConnection(int startIndex , int count , SqliteTransaction transaction , SqliteConnection connection)
    {
#pragma warning disable S2077
        using SqliteCommand removeEntries = new SqliteCommand(
                $"""
                    DELETE FROM LogEntries
                    WHERE Id >= {startIndex} AND Id < {startIndex + count};
                    """ , connection , transaction
            );
        removeEntries.ExecuteNonQuery();

        using SqliteCommand removeGap = new SqliteCommand(
                $"""
                    UPDATE LogEntries
                    SET Id = Id - {count}
                    WHERE Id >= {startIndex};
                    """ , connection , transaction
            );
        removeGap.ExecuteNonQuery();
#pragma warning restore S2077
    }

    private List<LogEntry> ReadFromDatabase()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        return ReadFromDatabaseUsingConnection(connection);
    }

    private List<LogEntry> ReadFromDatabaseUsingConnection(SqliteConnection connection , SqliteTransaction? transaction = null)
    {
        List<LogEntry> entries = [];
        using SqliteCommand readLogEntries = connection.CreateCommand();
        readLogEntries.CommandText = "SELECT Id , Guid , Term , Operation , LogType , Key , Value FROM LogEntries ORDER BY Id;";
        if (transaction is not null)
            readLogEntries.Transaction = transaction;
        using SqliteDataReader reader = readLogEntries.ExecuteReader();
        while (reader.Read())
        {
            Guid              guid            = reader.GetGuid(1);
            int               term            = reader.GetInt32(2);
            LogEntryOperation operation       = reader.GetFieldValue<LogEntryOperation>(3);
            string            logType         = reader.GetString(4);
            string?           key             = reader.IsDBNull(5) ? null : reader.GetFieldValue<string?>(5);
            string?           serializedValue = reader.IsDBNull(6) ? null : reader.GetString(6);
            entries.Add(LogEntry.Parse(guid , term , operation , logType , key , serializedValue));
        }
        CachedLogEntries = entries;
        return entries;
    }

    /// <summary> Overwrites the entire database by new entries. </summary>
    private void WriteToDatabase(IReadOnlyList<LogEntry> entriesToSerialize) => TryOverrideEntriesStartFromAndEraseFutureEntries(entriesToSerialize , 0);

    #region Interface implementations
    public void Add(LogEntry entry) => AddRange([entry]);

    public void Clear() => WriteToDatabase([]);

    public bool Contains(LogEntry item) => CachedLogEntries.Contains(item);

    public void CopyTo(LogEntry[] array , int arrayIndex) => new List<LogEntry>(CachedLogEntries).CopyTo(array , arrayIndex);

    public int IndexOf(LogEntry item) => new List<LogEntry>(CachedLogEntries).IndexOf(item);

    public void Insert(int index , LogEntry entry)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
#pragma warning disable S2077
            using SqliteCommand createSpace = new SqliteCommand(
                    $"""
                    UPDATE LogEntries
                    SET Id = Id + 1
                    WHERE Id >= {index}
                    """ , connection , transaction  // "ORDER BY" increments larger ID first to prevent ID collision (enforce unique constraint).
                );
            createSpace.ExecuteNonQuery();

            using SqliteCommand addEntry = new SqliteCommand(
                    $"""
                    INSERT INTO LogEntries (Id , Guid , Term , Operation , LogType , Key , Value)
                    VALUES (
                            {index} , $guid , $term , $operation , $logType , $key , $value
                        );
                    """ , connection , transaction
                );
            addEntry.Parameters.AddWithValue("$guid"      , entry.Guid.ToString());
            addEntry.Parameters.AddWithValue("$term"      , entry.Term           );
            addEntry.Parameters.AddWithValue("$operation" , (int)entry.Operation );
            addEntry.Parameters.AddWithValue("$logType"   , entry.GetLogType()   );
            addEntry.Parameters.AddWithValue("$key"       , entry.Key              ?? (object)DBNull.Value);
            addEntry.Parameters.AddWithValue("$value"     , entry.SerializeValue() ?? (object)DBNull.Value);
            addEntry.ExecuteNonQuery();
#pragma warning restore S2077

            transaction.Commit();
        }
        catch (Exception e)
        {
            standardTrace.TraceEvent(TraceEventType.Error , 0 , $"Cannot insert new log entry to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }

    public bool Remove(LogEntry item)
    {
        int index = new List<LogEntry>(CachedLogEntries).IndexOf(item);
        if (index == -1)
            return false;
        RemoveAt(index);
        return true;
    }

    public void RemoveAt(int index)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            RemoveEntriesInRangeViaTransactionConnection(index , 1  , transaction , connection);
            transaction.Commit();
        }
        catch (Exception e)
        {
            standardTrace.TraceEvent(TraceEventType.Error , 0 , $"Cannot remove log entries at {index}. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }


    public IEnumerator<LogEntry> GetEnumerator() => CachedLogEntries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    #endregion
}
