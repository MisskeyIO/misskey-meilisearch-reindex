namespace Misskey.Tools.MeiliSearch.Reindex;

public readonly record struct Note
{
    public string Id { get; init; }
    public long CreatedAt { get; init; }
    public string UserId { get; init; }
    public string? UserHost { get; init; }
    public string? ChannelId { get; init; }
    public string? Cw { get; init; }
    public string? Text { get; init; }
    public string[] Tags { get; init; }
}