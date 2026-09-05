using Microsoft.Data.Sqlite;


namespace RaftConsensusOnAspnet.RaftConsensus.Core.Misc;

public static class DbHelper
{
    public static SqliteConnection CreateNewConnection(string dbFilePath)
    {
        SqliteConnection connection = new SqliteConnection($"Data Source={dbFilePath}");
        connection.Open();
        using SqliteCommand pragma = connection.CreateCommand();
        pragma.CommandText = """
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            PRAGMA busy_timeout = 50;
            """;
        pragma.ExecuteNonQuery();
        return connection;
    }
}
