using System.Text.Json.Serialization;

namespace Manifest.Report.Classes
{
    public class BungieNewsRssResponse
    {
        [JsonPropertyName("Response")]
        public RssResponseItem Response { get; set; }
        [JsonPropertyName("ErrorCode")]
        public int ErrorCode { get; set; }
        [JsonPropertyName("ThrottleSeconds")]
        public int ThrottleSeconds { get; set; }
        [JsonPropertyName("ErrorStatus")]
        public string ErrorStatus { get; set; }
        [JsonPropertyName("Message")]
        public string Message { get; set; }
        [JsonPropertyName("MessageData")]
        public object MessageData { get; set; }
    }

    public class RssResponseItem
    {
        [JsonPropertyName("CurrentPaginationToken")]
        public int CurrentPaginationToken { get; set; }
        [JsonPropertyName("NextPaginationToken")]
        public int? NextPaginationToken { get; set; }
        [JsonPropertyName("ResultCountThisPage")]
        public int ResultCountThisPage { get; set; }
        [JsonPropertyName("PagerAction")]
        public string PagerAction { get; set; }
        [JsonPropertyName("NewsArticles")]
        public List<NewsArticleItem> NewsArticles { get; set; }
    }

    public class NewsArticleItem
    {
        [JsonPropertyName("Title")]
        public string Title { get; set; }
        [JsonPropertyName("Link")]
        public string Link { get; set; }
        [JsonPropertyName("PubDate")]
        public DateTimeOffset PubDate { get; set; }
        [JsonPropertyName("UniqueIdentifier")]
        public string UniqueIdentifier { get; set; }
        [JsonPropertyName("Description")]
        public string Description { get; set; }
        [JsonPropertyName("HtmlContent")]
        public string HtmlContent { get; set; }
        [JsonPropertyName("ImagePath")]
        public string ImagePath { get; set; }
        [JsonPropertyName("OptionalMobileImagePath")]
        public string? OptionalMobileImagePath { get; set; }
    }
}
