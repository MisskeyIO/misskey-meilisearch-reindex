using System.CommandLine;
using System.Data;
using System.Diagnostics;
using Meilisearch;
using Misskey.Tools.MeiliSearch.Reindex;
using Npgsql;

var databaseConnectionStringOption = new Option<string>(
    "--database",
    "-d"
) { Description = "Database Connection String, e.g. 'Server=localhost;Port=5432;Database=misskey;User Id=postgres;Password=postgres;'", Required = true };

var meiliSearchHostOption = new Option<string>(
    "--meili",
    "-m"
) { Description = "MeiliSearch Host, e.g. 'http://localhost:7700'", Required = true };

var meiliSearchKeyOption = new Option<string>(
    "--meili-key",
    "-k"
) { Description = "MeiliSearch API Key", Required = true };

var meiliSearchIndexOption = new Option<string>(
    "--meili-index",
    "-i"
) { Description = "MeiliSearch Index", Required = true };

var meiliSearchIndexSettingsUpdateOption = new Option<bool>(
    "--meili-index-settings-update",
    "-x"
) { Description = "Update MeiliSearch Index Settings" };

var indexSinceOption = new Option<DateTime?>(
    "--index-since",
    "-s"
) { Description = "Index Since" };

var indexUntilOption = new Option<DateTime?>(
    "--index-until",
    "-u"
) { Description = "Index Until" };

var batchSizeOption = new Option<int>(
    "--batch-size",
    "-n"
) { Description = "Batch Size", DefaultValueFactory = _ => 1000 };

var delayMillisecondsOption = new Option<int>(
    "--delay",
    "-p"
) { Description = "Delay milliseconds between indexing loop iterations", DefaultValueFactory = _ => 0 };

var additionalHostsOption = new Option<string[]>(
    "--additional-hosts",
    "-a"
) { Description = "Additional Hosts", AllowMultipleArgumentsPerToken = true };

var rootCommand = new RootCommand("Reindex Misskey Notes to MeiliSearch")
{
    databaseConnectionStringOption,
    meiliSearchHostOption,
    meiliSearchKeyOption,
    meiliSearchIndexOption,
    meiliSearchIndexSettingsUpdateOption,
    indexSinceOption,
    indexUntilOption,
    batchSizeOption,
    delayMillisecondsOption,
    additionalHostsOption
};

rootCommand.SetAction(async parseResult =>
    {
        var databaseConnectionString = parseResult.GetRequiredValue(databaseConnectionStringOption);
        var meiliSearchHost = parseResult.GetRequiredValue(meiliSearchHostOption);
        var meiliSearchKey = parseResult.GetRequiredValue(meiliSearchKeyOption);
        var meiliSearchIndex = parseResult.GetRequiredValue(meiliSearchIndexOption);
        var meiliSearchIndexSettingsUpdate = parseResult.GetValue(meiliSearchIndexSettingsUpdateOption);
        var indexSince = parseResult.GetValue(indexSinceOption);
        var indexUntil = parseResult.GetValue(indexUntilOption);
        var batchSize = parseResult.GetValue(batchSizeOption);
        var delayMilliseconds = parseResult.GetValue(delayMillisecondsOption);
        var additionalHosts = parseResult.GetValue(additionalHostsOption) ?? Array.Empty<string>();

        if (delayMilliseconds < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(delayMilliseconds), "Delay milliseconds must be greater than or equal to 0.");
        }

        var startupStopwatch = Stopwatch.StartNew();

        await using var dbConnection = new NpgsqlConnection(databaseConnectionString);
        await dbConnection.OpenAsync();

        var meiliSearchClient = new MeilisearchClient(meiliSearchHost, meiliSearchKey);
        var meiliSearchIndexClient = meiliSearchClient.Index(meiliSearchIndex);

        if (meiliSearchIndexSettingsUpdate)
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Updating MeiliSearch Index Settings...");
            var taskInfo = await meiliSearchIndexClient.UpdateSettingsAsync(new Settings
            {
                SearchableAttributes = new[] { "text", "cw" },
                FilterableAttributes = new[] { "channelId", "createdAt", "tags", "userHost", "userId" },
                SortableAttributes = new[] { "createdAt" },
                TypoTolerance = new TypoTolerance { Enabled = false },
                Pagination = new Pagination { MaxTotalHits = 10000 }
            });
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Updated MeiliSearch Index Settings | TaskId: {taskInfo.TaskUid:N0} {taskInfo.Status}");
        }

        string EncodeDateTimeToAid(DateTime dateTime)
        {
            const long time2000 = 946684800000L;
            const string digits = "0123456789abcdefghijklmnopqrstuvwxyz";
            var time = ((DateTimeOffset)dateTime).ToUnixTimeMilliseconds() - time2000;
            var encoded = string.Empty;
            do
            {
                encoded = digits[(int)(time % digits.Length)] + encoded;
            } while ((time /= digits.Length) != 0);

            return encoded.PadLeft(8, '0') + "00";
        }

        string GenerateQuerySince() =>
            indexSince.HasValue
                ? $"""
                    "note"."id" >= '{EncodeDateTimeToAid(indexSince.Value)}' and
                  """
                : string.Empty;

        string GenerateQueryUntil() =>
            indexUntil.HasValue
                ? $"""
                    "note"."id" <= '{EncodeDateTimeToAid(indexUntil.Value)}' and
                  """
                : string.Empty;

        string GenerateQueryUserHost() =>
            additionalHosts.Length == 0
                ? """
                  "note"."userHost" is null and
                  """
                : $"""
                   ("note"."userHost" is null or "note"."userHost" in ('{string.Join("', '", additionalHosts)}')) and
                  """;

        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Fetching total notes...");

        var countNotesQuery =
            $"""
              select
                  count(*)
              from
                  "public"."note"
              where
                  {GenerateQuerySince()} {GenerateQueryUntil()}
                  ("note"."visibility" = 'public' or "note"."visibility" = 'home') and
                  {GenerateQueryUserHost()}
                  (("note"."renoteId" is not null and "note"."text" is not null) or ("note"."renoteId" is null and "note"."text" is not null))
            """;

        async Task<long> FetchTotalNotes(NpgsqlConnection connection)
        {
            await using var countCommand = new NpgsqlCommand(countNotesQuery, connection);
            return (long?)await countCommand.ExecuteScalarAsync() ?? 0L;
        }

        var totalNotes = await FetchTotalNotes(dbConnection);
        var totalNotesRefreshStopwatch = Stopwatch.StartNew();

        if (totalNotes == 0)
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> No notes to index");
            return;
        }

        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Start indexing {totalNotes:N0} notes");

        var query =
            $"""
              select
                  "id", "createdAt", "userId", "userHost", "channelId", "cw", "text", "tags"
              from
                  "public"."note"
              where
                  {GenerateQuerySince()} {GenerateQueryUntil()}
                  ("note"."visibility" = 'public' or "note"."visibility" = 'home') and
                  {GenerateQueryUserHost()}
                  (("note"."renoteId" is not null and "note"."text" is not null) or ("note"."renoteId" is null and "note"."text" is not null)) and
                  "note"."id" > @cursor
              order by "note"."id"
              limit @limit
            """;

        var taskElapsedRecords = new Queue<long>();
        var taskElapsedStopwatch = new Stopwatch();

        var totalFetchedNotes = 0L;
        var cursor = "0000000000";
        int fetchedNotes;
        do
        {
            if (totalNotesRefreshStopwatch.ElapsedMilliseconds > 3_600_000) // 1 hour
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Fetching total notes...");
                totalNotes = await FetchTotalNotes(dbConnection);
                totalNotesRefreshStopwatch.Restart();
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Updated total notes: {totalNotes:N0}");
            }

            taskElapsedStopwatch.Restart();
            await using var cmd = new NpgsqlCommand(query, dbConnection);
            cmd.Parameters.AddWithValue("limit", batchSize);
            cmd.Parameters.AddWithValue("cursor", cursor);
            await using var reader = await cmd.ExecuteReaderAsync();
            var documents = new List<Note>();
            while (await reader.ReadAsync())
            {
                var note = new Note
                {
                    Id = reader.GetString("id"),
                    CreatedAt = ((DateTimeOffset)reader.GetDateTime("createdAt").ToUniversalTime()).ToUnixTimeMilliseconds(),
                    UserId = reader.GetString("userId"),
                    UserHost = reader.IsDBNull("userHost") ? null : reader.GetString("userHost"),
                    ChannelId = reader.IsDBNull("channelId") ? null : reader.GetString("channelId"),
                    Cw = reader.IsDBNull("cw") ? null : reader.GetString("cw"),
                    Text = reader.IsDBNull("text") ? null : reader.GetString("text"),
                    Tags = reader.IsDBNull("tags") ? Array.Empty<string>() : reader.GetFieldValue<string[]>("tags")
                };
                documents.Add(note);
            }

            fetchedNotes = documents.Count;
            totalFetchedNotes += fetchedNotes;
            cursor = documents.LastOrDefault().Id;
            var cursorDateTime = DateTimeOffset.FromUnixTimeMilliseconds(documents.LastOrDefault().CreatedAt).ToOffset(TimeSpan.FromHours(9));
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Fetched {fetchedNotes:N0} notes from DB | {totalFetchedNotes:N0} / {totalNotes:N0} | cursor: {cursor} {cursorDateTime:yyyy-MM-dd HH:mm:ss}");

            if (documents.Count == 0) break;
            var taskInfo = await meiliSearchIndexClient.AddDocumentsAsync(documents, "id");
            Console.Write($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Index {fetchedNotes:N0} notes to MeiliSearch | TaskId: {taskInfo.TaskUid:N0} {taskInfo.Status}");
            taskElapsedStopwatch.Stop();

            taskElapsedRecords.Enqueue(taskElapsedStopwatch.ElapsedMilliseconds);
            if (taskElapsedRecords.Count > 10) taskElapsedRecords.Dequeue();

            var estimatedRemainingTime = TimeSpan.FromMilliseconds(taskElapsedRecords.Average() * (totalNotes - totalFetchedNotes) / batchSize);
            var progress = (double) totalFetchedNotes / totalNotes * 100;

            Console.WriteLine($" | Progress: {progress:F2}% | Estimated Remaining Time: {estimatedRemainingTime:hh\\:mm\\:ss}");

            if (delayMilliseconds > 0 && fetchedNotes == batchSize)
            {
                await Task.Delay(delayMilliseconds);
            }
        } while (fetchedNotes == batchSize);

        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}> Finish indexing {totalFetchedNotes:N0} notes | elapsed: {startupStopwatch.Elapsed:hh\\:mm\\:ss}");
    }
);

return await rootCommand.Parse(args).InvokeAsync();
