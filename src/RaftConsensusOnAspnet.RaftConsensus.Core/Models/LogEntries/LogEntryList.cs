using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class LogEntryList : IList<LogEntry> , IReadOnlyList<LogEntry>
{
    public LogEntry this[int index]
    {
        get => CachedLogEntries[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => CachedLogEntries.Count;
    public int LastAppliedIndex { get; private set; } = -1;
    [NotNull] private IReadOnlyList<LogEntry>? CachedLogEntries
    {
        get => field ??= ReadFromDatabase();
        set => field = value is null ? null : field;
    }

    public const string StateMachineValuesTableName = "StateMachineValues";
    private const int StateMachineValueSyncCooldownInterval = 500;

    private readonly string dbFilePath;
    private TaskCompletionSource? stopStateMachineSyncTcs;


    public LogEntryList(string filePath , bool clearEntries = false)
    {
        dbFilePath = filePath;
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);

        using SqliteCommand createLogEntryMetadata = new SqliteCommand(
                """
                CREATE TABLE IF NOT EXISTS LogEntryMeta (
                    Key   TEXT ,
                    Value TEXT
                );
                """ , connection
            );
        createLogEntryMetadata.ExecuteNonQuery();

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

        if (clearEntries)
            Clear();
    }


    #region Parallel services
    public async Task StartSynchronizingStateMachineValueAsync()
    {
        stopStateMachineSyncTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task stop = stopStateMachineSyncTcs.Task;
        while (true)
        {
            await Task.Run(ApplyEntries);

            Task finishedTask = await Task.WhenAny(Task.Delay(StateMachineValueSyncCooldownInterval) , stop);
            if (finishedTask == stop)
                break;
        }
    }

    public void StopSynchronizingStateMachineValue() => stopStateMachineSyncTcs?.TrySetResult();
    #endregion

    public void AddRange(IEnumerable<LogEntry> newEntries)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand addEntry = new SqliteCommand(
                    """
                    INSERT INTO LogEntries (Id , Guid , Term , Operation , LogType , Key , Value)
                    VALUES (
                            (SELECT MAX(Id) FROM LogEntries) + 1,
                            $guid , $term , $operation , $logType , $key , $value
                        );
                    """ , connection , transaction
                );
            SqliteParameter varGuid      = addEntry.Parameters.Add("$guid"      , SqliteType.Text   );
            SqliteParameter varTerm      = addEntry.Parameters.Add("$term"      , SqliteType.Integer);
            SqliteParameter varOperation = addEntry.Parameters.Add("$operation" , SqliteType.Integer);
            SqliteParameter varLogType   = addEntry.Parameters.Add("$logType"   , SqliteType.Text   );
            SqliteParameter varKey       = addEntry.Parameters.Add("$key"       , SqliteType.Text   );
            SqliteParameter varValue     = addEntry.Parameters.Add("$value"     , SqliteType.Text   );
            foreach (LogEntry entry in newEntries)
            {
                varGuid.Value = entry.Guid.ToString();
                varTerm.Value = entry.Term;
                varOperation.Value = (int)entry.Operation;
                varLogType.Value = entry.GetLogType();
                varKey.Value = entry.Key ?? (object)DBNull.Value;
                varValue.Value = entry.SerializeValue() ?? (object)DBNull.Value;
                addEntry.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot add new log entry to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }

    public void ApplyEntries()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand deleteExistValues = new SqliteCommand($"DELETE FROM {StateMachineValuesTableName};" , connection , transaction);
            deleteExistValues.ExecuteNonQuery();

            using SqliteCommand applyEntries = new SqliteCommand(
                    $"""
                    INSERT INTO {StateMachineValuesTableName} (Key , Value , Type)
                    SELECT Key , Value , LogType AS Type FROM (
                        SELECT MAX(Id) AS Id , Term , Operation , LogType , Key , Value FROM LogEntries
                        WHERE Key IS NOT NULL
                        GROUP BY Key
                        ORDER BY Id DESC
                    )
                    WHERE Operation != {(int)LogEntryOperation.Delete}
                    """ , connection , transaction
                );
            applyEntries.ExecuteNonQuery();

            using SqliteCommand getMaxAppliedIndex = new SqliteCommand("SELECT MAX(Id) FROM LogEntries;" , connection , transaction);
            int maxAppliedIndex = Convert.ToInt32(getMaxAppliedIndex.ExecuteScalar());

            transaction.Commit();

            LastAppliedIndex = maxAppliedIndex;
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot apply log entries to state machine database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
    }

    /// <remarks> <b>REMARKS:</b> Removes all entries after startIndex. </remarks>
    public bool TryEraseAndAppendEntriesAt(IReadOnlyList<LogEntry> entriesToAppend , int startIndex)
    {
        if (Count < startIndex)
        {
            Trace.TraceWarning("Append entries failed because of missing logs");
            return false;
        }

        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
#pragma warning disable S2077
            using SqliteCommand removeEntriesAfterStartIndex = new SqliteCommand(
                    $"""
                    DELETE FROM LogEntries
                    WHERE Id >= {startIndex};
                    """ , connection , transaction
                );
            removeEntriesAfterStartIndex.ExecuteNonQuery();
#pragma warning restore S2077

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
            Trace.TraceError($"Cannot insert new log entry (start at {startIndex}) to database. Error: \n{e}");
            transaction.Rollback();
            return false;
        }
        CachedLogEntries = null;
        return true;
    }

    public void RemoveRange(int start , int count)
    {
        if (start >= CachedLogEntries.Count)
            return;

        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
#pragma warning disable S2077
            using SqliteCommand removeEntries = new SqliteCommand(
                    $"""
                    DELETE FROM LogEntries
                    WHERE Id >= {start} AND Id < {start + count};
                    """ , connection , transaction
                );
            removeEntries.ExecuteNonQuery();

            using SqliteCommand removeGap = new SqliteCommand(
                    $"""
                    UPDATE LogEntries
                    SET Id = Id - {count}
                    WHERE Id >= {start};
                    """ , connection , transaction
                );
            removeGap.ExecuteNonQuery();
#pragma warning restore S2077

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot remove log entries from {start} to {start + count}(Exclusive). Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
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
            Guid guid = reader.GetGuid(1);
            int term = reader.GetInt32(2);
            LogEntryOperation operation = reader.GetFieldValue<LogEntryOperation>(3);
            string logType = reader.GetString(4);
            string? key = reader.IsDBNull(5) ? null : reader.GetFieldValue<string?>(5);
            string? serializedValue = reader.IsDBNull(6) ? null : reader.GetString(6);
            entries.Add(LogEntry.Parse(guid , term , operation , logType , key , serializedValue));
        }
        CachedLogEntries = entries;
        return entries;
    }

    /// <summary> Overwrites the entire database by new entries. </summary>
    private void WriteToDatabase(IEnumerable<LogEntry> entriesToSerialize)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand emptyLogEntries = connection.CreateCommand();
            emptyLogEntries.Transaction = transaction;
            emptyLogEntries.CommandText = "DELETE FROM LogEntries;";
            emptyLogEntries.ExecuteNonQuery();

            using SqliteCommand addEntry = connection.CreateCommand();
            addEntry.Transaction = transaction;
            addEntry.CommandText = """
                INSERT INTO LogEntries (Id , Guid , Term , Operation , LogType , Key , Value)
                VALUES (
                        (SELECT COUNT(*) FROM LogEntries) ,
                        $guid , $term , $operation , $logType , $key , $value
                    );
                """;
            SqliteParameter varGuid      = addEntry.Parameters.Add("$guid"      , SqliteType.Text   );
            SqliteParameter varTerm      = addEntry.Parameters.Add("$term"      , SqliteType.Integer);
            SqliteParameter varOperation = addEntry.Parameters.Add("$operation" , SqliteType.Integer);
            SqliteParameter varLogType   = addEntry.Parameters.Add("$logType"   , SqliteType.Text   );
            SqliteParameter varKey       = addEntry.Parameters.Add("$key"       , SqliteType.Text   );
            SqliteParameter varValue     = addEntry.Parameters.Add("$value"     , SqliteType.Text   );
            foreach (LogEntry entry in entriesToSerialize)
            {
                varGuid.Value = entry.Guid.ToString();
                varTerm.Value = entry.Term;
                varOperation.Value = (int)entry.Operation;
                varLogType.Value = entry.GetLogType();
                varKey.Value = entry.Key ?? (object)DBNull.Value;
                varValue.Value = entry.SerializeValue() ?? (object)DBNull.Value;
                addEntry.ExecuteNonQuery();
            }

            CachedLogEntries = null;
            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot write new entries to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }

    #region Interface implementations
    public void Add(LogEntry entry)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand addEntry = new SqliteCommand(
                    """
                    INSERT INTO LogEntries (Id , Guid , Term , Operation , LogType , Key , Value)
                    VALUES (
                            COALESCE(
                                    (SELECT MAX(Id) FROM LogEntries) + 1 , 0
                                ) ,
                            $guid , $term , $operation , $logType , $key , $value
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
            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot add new log entry to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }

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
            Trace.TraceError($"Cannot insert new log entry to database. Error: \n{e}");
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
#pragma warning disable S2077
            using SqliteCommand removeEntry = new SqliteCommand(
                    $"""
                    DELETE FROM LogEntries
                    WHERE Id = {index};
                    """ , connection , transaction
                );
            removeEntry.ExecuteNonQuery();

            using SqliteCommand removeGap = new SqliteCommand(
                    $"""
                    UPDATE LogEntries
                    SET Id = Id - 1
                    WHERE Id > {index};
                    """ , connection , transaction
                );
            removeGap.ExecuteNonQuery();
#pragma warning restore S2077

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot remove log entries at {index}. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
        CachedLogEntries = null;
    }


    public IEnumerator<LogEntry> GetEnumerator() => CachedLogEntries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    #endregion
}
