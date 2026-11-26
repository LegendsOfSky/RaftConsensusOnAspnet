using System.Diagnostics;
using Microsoft.Data.Sqlite;
using RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;
using RaftConsensusOnAspnet.RaftConsensus.Core.Miscs;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Data;

public class RaftNodeMetadataSqliteStorage : IRaftNodeMetadataPersistentStorage
{
    public int CurrentTerm
    {
        get => _currentTerm ??= ReadCurrentTermFromDb();
        set
        {
            WriteCurrentTermToDb(value);
            _currentTerm = null;
        }
    }
    public (Guid VoteFor , int TermOfVote) VoteInfo
    {
        get => _voteFor ??= ReadVoteInfoFromDb();
        set
        {
            WriteVoteInfoToDb(value.VoteFor , value.TermOfVote);
            _voteFor = null;
        }
    }

    private const string TableName = "RaftNodeState";

    private readonly string dbFilePath;
    private readonly TraceSource debugTrace;
    private readonly TraceSource standardTrace;
    private int? _currentTerm;
    private (Guid VoteFor , int TermOfVote)? _voteFor;


    public RaftNodeMetadataSqliteStorage(string dbFilePathIn ,  Guid nodeId ,
                                         IEnumerable<TraceListener>? standardTraceListenersIn = null , IEnumerable<TraceListener>? debugTraceListenersIn = null)
    {
        dbFilePath = dbFilePathIn;
        CreateDatabaseSchemaIfNotExist();

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


    public void RemoveExistingData()
    {
        const string TraceMessage = "Remove existing raft node metadata.";
        standardTrace.TraceEvent(TraceEventType.Information , 0 , TraceMessage);
        debugTrace.TraceEvent(TraceEventType.Information , 0 , TraceMessage);

        CurrentTerm = 0;
        VoteInfo = (Guid.Empty , 0);
    }

    private void CreateDatabaseSchemaIfNotExist()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand createTable = connection.CreateCommand();
        createTable.CommandText = $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                Key   TEXT PRIMARY KEY ,
                Value TEXT
            );
            """;
        createTable.ExecuteNonQuery();
    }

    private int ReadCurrentTermFromDb()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand getCurrentTerm = new SqliteCommand(
                $"""
                SELECT Value
                FROM {TableName}
                WHERE Key = 'CurrentTerm';
                """ , connection
            );
        using SqliteDataReader reader = getCurrentTerm.ExecuteReader();
        return reader.Read() ? int.Parse(reader.GetString(0)) : 0;
    }

    private (Guid VoteFor , int TermOfVote) ReadVoteInfoFromDb()
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteCommand getVoteInfo = new SqliteCommand(
                $"""
                SELECT Key , Value
                FROM {TableName}
                WHERE Key = 'VoteFor' OR Key = 'TermOfVote';
                """ , connection
            );
        using SqliteDataReader reader = getVoteInfo.ExecuteReader();
        (Guid voteFor , int termOfVote) = (Guid.Empty , -1);
        while (reader.Read())
        {
            string key = reader.GetString(0);
            if (key == "VoteFor")
                voteFor = reader.GetGuid(1);
            else if (key == "TermOfVote")
                termOfVote = int.Parse(reader.GetString(1));
        }
        return voteFor == Guid.Empty || termOfVote == -1
            ? (Guid.Empty , -1)
            : (voteFor , termOfVote);
    }

    private void WriteCurrentTermToDb(int currentTerm)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand getCurrentTerm = new SqliteCommand(
                    $"""
                    SELECT Key , Value
                    FROM {TableName}
                    WHERE Key = 'CurrentTerm';
                    """ , connection , transaction
                );
            using SqliteDataReader reader = getCurrentTerm.ExecuteReader();

            using SqliteCommand setCurrentTerm = new SqliteCommand(
                    reader.Read()
                        ? $"""
                            UPDATE RaftNodeState
                            SET Value = '{currentTerm}'
                            WHERE Key = 'CurrentTerm';
                            """
                        : $"""
                            INSERT INTO RaftNodeState (Key , Value)
                            VALUES ('CurrentTerm' , '{CurrentTerm}');
                            """ ,
                    connection , transaction
                );
            setCurrentTerm.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot write new entries to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
    }

    private void WriteVoteInfoToDb(Guid voteFor , int termOfVote)
    {
        using SqliteConnection connection = DbHelper.CreateNewConnection(dbFilePath);
        using SqliteTransaction transaction = connection.BeginTransaction();
        try
        {
            using SqliteCommand getVoteFor = new SqliteCommand(
                    $"""
                    SELECT Key
                    FROM {TableName}
                    WHERE Key = 'VoteFor';
                    """ , connection , transaction
                );
            using SqliteDataReader getVoteForReader = getVoteFor.ExecuteReader();
            using SqliteCommand setVoteFor = new SqliteCommand(
                    getVoteForReader.Read()
                        ?  $"""
                            UPDATE {TableName}
                            SET Value = '{voteFor}'
                            WHERE Key = 'VoteFor';
                            """
                        :  $"""
                            INSERT INTO {TableName} (Key , Value)
                            VALUES ('VoteFor' , '{voteFor}');
                            """ ,
                    connection , transaction
                );
            setVoteFor.ExecuteNonQuery();

            using SqliteCommand getTermOfVote = new SqliteCommand(
                    $"""
                    SELECT Key
                    FROM {TableName}
                    WHERE Key = 'TermOfVote';
                    """ , connection , transaction
                );
            using SqliteDataReader getTermOfVoteReader = getTermOfVote.ExecuteReader();
            using SqliteCommand setTermOfVote = new SqliteCommand(
                    getTermOfVoteReader.Read()
                        ? $"""
                            UPDATE {TableName}
                            SET Value = '{termOfVote}'
                            WHERE Key = 'TermOfVote';
                            """
                        : $"""
                            INSERT INTO {TableName} (Key , Value)
                            VALUES ('TermOfVote' , '{termOfVote}');
                            """ ,
                    connection , transaction
                );
            setTermOfVote.ExecuteNonQuery();

            transaction.Commit();
        }
        catch (Exception e)
        {
            Trace.TraceError($"Cannot write new entries to database. Error: \n{e}");
            transaction.Rollback();
            throw;
        }
    }
}
