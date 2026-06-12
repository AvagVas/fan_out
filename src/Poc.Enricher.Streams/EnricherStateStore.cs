using Microsoft.Data.Sqlite;

namespace Poc.Enricher.Streams;

/// <summary>
/// Durable local state store backing the manual KTable-KTable join. SQLite plays the role RocksDB
/// plays in Kafka Streams: it materializes each input topic as a latest-value-per-key table and
/// persists an "emitted" ledger that makes output idempotent and restart-safe.
///
/// Tables:
///   requests(correlation_id PK, value_json)     -- materialized proxy.requests KTable
///   completions(correlation_id PK, value_json)  -- materialized service-a.completed KTable
///   emitted(correlation_id PK, emitted_at_utc)  -- dedup / idempotency ledger
///
/// A separate read connection (WAL mode) serves metric counts without contending with the writer.
/// </summary>
public sealed class EnricherStateStore : IDisposable
{
    private readonly SqliteConnection _write;
    private readonly SqliteConnection _read;
    private readonly object _readLock = new();

    public EnricherStateStore(string databasePath)
    {
        var dir = Path.GetDirectoryName(Path.GetFullPath(databasePath));
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
        }.ToString();

        _write = new SqliteConnection(connectionString);
        _write.Open();
        Execute("PRAGMA journal_mode=WAL;");
        Execute("PRAGMA synchronous=FULL;"); // durability: flush on commit so restart never loses state
        Execute("""
            CREATE TABLE IF NOT EXISTS requests (correlation_id TEXT PRIMARY KEY, value_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS completions (correlation_id TEXT PRIMARY KEY, value_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS emitted (correlation_id TEXT PRIMARY KEY, emitted_at_utc TEXT NOT NULL);
        """);

        _read = new SqliteConnection(connectionString);
        _read.Open();
    }

    public SqliteTransaction BeginTransaction() => _write.BeginTransaction();

    public void UpsertRequest(string key, string valueJson, SqliteTransaction txn) =>
        Upsert("requests", key, valueJson, txn);

    public void UpsertCompletion(string key, string valueJson, SqliteTransaction txn) =>
        Upsert("completions", key, valueJson, txn);

    public string? TryGetRequest(string key, SqliteTransaction txn) => TryGet("requests", key, txn);

    public string? TryGetCompletion(string key, SqliteTransaction txn) => TryGet("completions", key, txn);

    public bool IsEmitted(string key, SqliteTransaction txn)
    {
        using var cmd = _write.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "SELECT 1 FROM emitted WHERE correlation_id = $k LIMIT 1;";
        cmd.Parameters.AddWithValue("$k", key);
        return cmd.ExecuteScalar() is not null;
    }

    public void MarkEmitted(string key, DateTimeOffset emittedAtUtc, SqliteTransaction txn)
    {
        using var cmd = _write.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = "INSERT OR IGNORE INTO emitted (correlation_id, emitted_at_utc) VALUES ($k, $t);";
        cmd.Parameters.AddWithValue("$k", key);
        cmd.Parameters.AddWithValue("$t", emittedAtUtc.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public long CountRequests() => Count("requests");

    public long CountCompletions() => Count("completions");

    public long CountEmitted() => Count("emitted");

    private void Upsert(string table, string key, string valueJson, SqliteTransaction txn)
    {
        using var cmd = _write.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = $"""
            INSERT INTO {table} (correlation_id, value_json) VALUES ($k, $v)
            ON CONFLICT(correlation_id) DO UPDATE SET value_json = excluded.value_json;
        """;
        cmd.Parameters.AddWithValue("$k", key);
        cmd.Parameters.AddWithValue("$v", valueJson);
        cmd.ExecuteNonQuery();
    }

    private string? TryGet(string table, string key, SqliteTransaction txn)
    {
        using var cmd = _write.CreateCommand();
        cmd.Transaction = txn;
        cmd.CommandText = $"SELECT value_json FROM {table} WHERE correlation_id = $k LIMIT 1;";
        cmd.Parameters.AddWithValue("$k", key);
        return cmd.ExecuteScalar() as string;
    }

    private long Count(string table)
    {
        lock (_readLock)
        {
            using var cmd = _read.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {table};";
            return (long)(cmd.ExecuteScalar() ?? 0L);
        }
    }

    private void Execute(string sql)
    {
        using var cmd = _write.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _read.Dispose();
        _write.Dispose();
        SqliteConnection.ClearAllPools(); // release the file handle so a restart in tests can reopen
    }
}
