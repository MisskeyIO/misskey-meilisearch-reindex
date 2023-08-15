using System.CommandLine;
using System.Data;
using Meilisearch;
using Misskey.Tools.MeiliSearch.Reindex;
using Npgsql;

var databaseConnectionStringOption = new Option<string>(
    "--database",
    "Database Connection String, e.g. 'Server=localhost;Port=5432;Database=misskey;User Id=postgres;Password=postgres;'"
) { IsRequired = true };
databaseConnectionStringOption.AddAlias("-c");

var meiliSearchHostOption = new Option<string>(
    "--meili",
    "MeiliSearch Host, e.g. 'http://localhost:7700'"
) { IsRequired = true };
meiliSearchHostOption.AddAlias("-s");

var meiliSearchKeyOption = new Option<string>(
    "--meili-key",
    "MeiliSearch API Key"
) { IsRequired = true };
meiliSearchKeyOption.AddAlias("-k");

var meiliSearchIndexOption = new Option<string>(
    "--meili-index",
    "MeiliSearch Index"
) { IsRequired = true };
meiliSearchIndexOption.AddAlias("-i");

var batchSizeOption = new Option<int>(
    "--batch-size",
    description: "Batch Size",
    getDefaultValue: () => 10000
) { IsRequired = false };
batchSizeOption.AddAlias("-n");

var additionalHostsOption = new Option<string[]>(
    "--additional-hosts",
    "Additional Hosts"
) { IsRequired = false, AllowMultipleArgumentsPerToken = true };
additionalHostsOption.AddAlias("-a");

var rootCommand = new RootCommand("Reindex Misskey Notes to MeiliSearch")
{
    databaseConnectionStringOption,
    meiliSearchHostOption,
    meiliSearchKeyOption,
    meiliSearchIndexOption,
    batchSizeOption,
    additionalHostsOption
};

rootCommand.SetHandler(async (
        databaseConnectionString,
        meiliSearchHost,
        meiliSearchKey,
        meiliSearchIndex,
        batchSize,
        additionalHosts
    ) =>
    {
        await using var dbConnection = new NpgsqlConnection(databaseConnectionString);
        await dbConnection.OpenAsync();

        var meiliSearchClient = new MeilisearchClient(meiliSearchHost, meiliSearchKey);
        var meiliSearchIndexClient = meiliSearchClient.Index(meiliSearchIndex);

        var query = additionalHosts.Length == 0
            ? """
              select
                  "id", "createdAt", "userId", "userHost", "channelId", "cw", "text", "tags"
              from
                  "public"."note"
              where
                  ("note"."visibility" = 'public' or "note"."visibility" = 'home') and
                  "note"."userHost" is null and
                  (("note"."renoteId" is not null and "note"."text" is not null) or ("note"."renoteId" is null and "note"."text" is not null))
              order by "note"."id"
              limit @limit offset @offset
              """
            : $"""
               select
                   "id", "createdAt", "userId", "userHost", "channelId", "cw", "text", "tags"
               from
                   "public"."note"
               where
                   ("note"."visibility" = 'public' or "note"."visibility" = 'home') and
                   ("note"."userHost" is null and "note"."userHost" in ("{
                       string.Join(
                           """
                            ", "
                           """,
                           additionalHosts
                       )
                   }")) and
                   (("note"."renoteId" is not null and "note"."text" is not null) or ("note"."renoteId" is null and "note"."text" is not null))
               order by "note"."id"
               limit @limit offset @offset
               """;

        Console.WriteLine("Start");

        int fetchedRows;
        var offset = 0L;
        do
        {
            await using var cmd = new NpgsqlCommand(query, dbConnection);
            cmd.Parameters.AddWithValue("limit", batchSize);
            cmd.Parameters.AddWithValue("offset", offset);
            await using var reader = await cmd.ExecuteReaderAsync();
            var documents = new List<Note>();
            while (await reader.ReadAsync())
            {
                var note = new Note
                {
                    Id = reader.GetString("id"),
                    CreatedAt = ((DateTimeOffset)reader.GetDateTime("createdAt").ToUniversalTime())
                        .ToUnixTimeMilliseconds(),
                    UserId = reader.GetString("userId"),
                    UserHost = reader.IsDBNull("userHost") ? null : reader.GetString("userHost"),
                    ChannelId = reader.IsDBNull("channelId") ? null : reader.GetString("channelId"),
                    Cw = reader.IsDBNull("cw") ? null : reader.GetString("cw"),
                    Text = reader.IsDBNull("text") ? null : reader.GetString("text"),
                    Tags = reader.IsDBNull("tags") ? Array.Empty<string>() : reader.GetFieldValue<string[]>("tags")
                };
                documents.Add(note);
            }

            offset += batchSize;
            fetchedRows = documents.Count;
            Console.WriteLine($"Fetched {fetchedRows} rows, offset: {offset}");

            if (documents.Count == 0) break;
            var taskInfo = await meiliSearchIndexClient.AddDocumentsAsync(documents, "id");
            Console.WriteLine($"TaskId: {taskInfo.TaskUid} {taskInfo.Status}");
        } while (fetchedRows == batchSize);

        Console.WriteLine("Done");
    },
    databaseConnectionStringOption,
    meiliSearchHostOption,
    meiliSearchKeyOption,
    meiliSearchIndexOption,
    batchSizeOption,
    additionalHostsOption
);

return await rootCommand.InvokeAsync(args);
