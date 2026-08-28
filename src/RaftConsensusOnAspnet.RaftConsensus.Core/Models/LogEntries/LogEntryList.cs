using System.Collections;
using System.Diagnostics;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;

public class LogEntryList : IList<LogEntry> , IReadOnlyList<LogEntry>
{  // TODO perform all operation using pure database command
    public LogEntry this[int index]
    {
        get => ReadFromDatabase()[index];
        set => throw new NotSupportedException();
    }
    public bool IsReadOnly => false;
    public int Count => ReadFromDatabase().Count;

    private readonly string dbFilePath;


    public LogEntryList(string filePath , bool clearEntries = false)
    {
        dbFilePath = filePath;
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand createTable = connection.CreateCommand();
        createTable.CommandText = """
            CREATE TABLE IF NOT EXISTS LogEntries (
                Id        INTEGER PRIMARY KEY AUTOINCREMENT ,
                Guid      TEXT    NOT NULL UNIQUE ,
                Term      INTEGER NOT NULL ,
                Operation INTEGER NOT NULL ,
                LogType   TEXT    NOT NULL ,
                Key       TEXT ,
                Value     TEXT
            );
            """;  // FIXME auto increment ID will not decrement once the last entry is removed.
        createTable.ExecuteNonQuery();
        connection.Close();  // FIXME remove .Close()

        if (clearEntries)
            Clear();
    }


    public void AddRange(IEnumerable<LogEntry> newEntries)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        persistentEntries.AddRange(newEntries);
        WriteToDatabase(persistentEntries);
    }

    public bool AppendEntriesAt(IReadOnlyList<LogEntry> entriesToAppend , int startIndex)
    {
        if (Count > startIndex)
            RemoveRange(startIndex , Count - startIndex);
        else if (Count < startIndex)
            throw new InvalidOperationException("Append entries failed because of missing logs.");

        AddRange(entriesToAppend);
        return true;
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
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
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
                INSERT INTO LogEntries (Guid , Term , Operation , LogType , Key , Value)
                VALUES ($guid , $term , $operation , $logType , $key , $value);
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

            transaction.Commit();
        }
        catch
        {
            Trace.WriteLine("Cannot write new entries to database.");
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

    public void Clear() => WriteToDatabase([]);

    public bool Contains(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        return persistentEntries.Contains(item);
    }

    public void CopyTo(LogEntry[] array , int arrayIndex) => ReadFromDatabase().CopyTo(array , arrayIndex);

    public int IndexOf(LogEntry item)
    {
        List<LogEntry> persistentEntries = ReadFromDatabase();
        return persistentEntries.IndexOf(item);
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


    public IEnumerator<LogEntry> GetEnumerator() => ReadFromDatabase().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    #endregion
}
