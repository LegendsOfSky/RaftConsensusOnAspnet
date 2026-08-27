using System.Collections;
using Microsoft.Data.Sqlite;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class LogEntryList : IList<LogEntry> , IReadOnlyList<LogEntry> , IDisposable , IAsyncDisposable
{
    public LogEntry this[int index]
    {
        get => ReadFromDatabase()[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => ReadFromDatabase().Count;

    private readonly SqliteConnection connection;
    private bool disposed;


    public LogEntryList(Guid nodeIdIn , bool clearEntries = false)
    {
        string filePath = $"{RaftNode.DataPath}/{nodeIdIn}.db";

        connection = new SqliteConnection($"Data Source={filePath}");
        connection.Open();
        using SqliteCommand pragma = connection.CreateCommand();
        pragma.CommandText = """
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            PRAGMA busy_timeout = 100;
            """;
        pragma.ExecuteNonQuery();
        using SqliteCommand createTable = connection.CreateCommand();
        createTable.CommandText = """
            CREATE TABLE IF NOT EXISTS LogEntries (
                Id        INTEGER PRIMARY KEY AUTOINCREMENT ,
                Guid      TEXT    NOT NULL ,
                Term      INTEGER NOT NULL ,
                Operation INTEGER NOT NULL ,
                LogType   TEXT    NOT NULL ,
                Key       TEXT ,
                Value     TEXT
            );
            """;
        createTable.ExecuteNonQuery();

        if (clearEntries)
            Clear();
    }


    public void AddRange(IEnumerable<LogEntry> newEntries)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.AddRange(newEntries);
        WriteToDatabase(persistentEntries);
    }

    public void RemoveRange(int start , int count)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.RemoveRange(start , count);
        WriteToDatabase(persistentEntries);
    }

    private List<LogEntry> ReadFromDatabase()
    {
        List<LogEntry> entries = [];
        using SqliteCommand readLogEntries = connection.CreateCommand();
        readLogEntries.CommandText = "SELECT Id , Guid , Term , Operation , LogType , Key , Value FROM LogEntries ORDER BY Id;";
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
        return entries;
    }

    private void WriteToDatabase(IEnumerable<LogEntry> entriesToSerialize)
    {
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
                INSERT INTO LogEntries (Guid , Term , Operation , LogType , Key , Value)
                VALUES ($Guid , $term , $operation , $logType , $key , $value);
                """;
            SqliteParameter varGuid      = addEntry.Parameters.Add("$Guid"      , SqliteType.Text   );
            SqliteParameter varTerm      = addEntry.Parameters.Add("$term"      , SqliteType.Integer);
            SqliteParameter varOperation = addEntry.Parameters.Add("$operation" , SqliteType.Integer);
            SqliteParameter varLogType   = addEntry.Parameters.Add("$logType"   , SqliteType.Text   );
            SqliteParameter varKey       = addEntry.Parameters.Add("$key"       , SqliteType.Text   );
            SqliteParameter varValue     = addEntry.Parameters.Add("$value"     , SqliteType.Text   );
            foreach (LogEntry entry in entriesToSerialize)
            {
                varGuid.Value = entry.Guid.ToString();
                varTerm.Value = entry.Term;
                varOperation.Value = entry.Operation;
                varLogType.Value = entry.GetLogType();
                varKey.Value = entry.Key ?? (object)DBNull.Value;
                varValue.Value = entry.SerializeValue() ?? (object)DBNull.Value;
                addEntry.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }


    #region Interface implementations
    public void Add(LogEntry entry)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.Add(entry);
        WriteToDatabase(persistentEntries);
    }

    public void Insert(int index , LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.Insert(index , item);
        WriteToDatabase(persistentEntries);
    }

    public bool Remove(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        bool success = persistentEntries.Remove(item);
        WriteToDatabase(persistentEntries);
        return success;
    }

    public void RemoveAt(int index)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.RemoveAt(index);
        WriteToDatabase(persistentEntries);
    }

    public void Clear()
    {
        WriteToDatabase([]);
    }

    public int IndexOf(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        return persistentEntries.IndexOf(item);
    }

    public bool Contains(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        return persistentEntries.Contains(item);
    }

    public void CopyTo(LogEntry[] array , int arrayIndex)
    {
        ReadFromDatabase().CopyTo(array , arrayIndex);
    }

    public IEnumerator<LogEntry> GetEnumerator() => ReadFromDatabase().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposed)
            return;

        if (disposing)
            connection.Dispose();

        disposed = true;
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        if (disposed)
            return;

        await connection.DisposeAsync().ConfigureAwait(false);
        disposed = true;
    }
    #endregion


    ~LogEntryList()
    {
        Dispose(false);
    }
}
