using Amazon.S3;
using Amazon.S3.Model;
using Hangfire.Server;
using Manifest.Report.Classes;
using System.Net;
using System.Text.Json;

namespace Manifest.Report.Jobs
{
    public class StoreNewsArticles(IHttpClientFactory httpClientFactory, AmazonS3Client s3Client)
    {
        private HttpClient httpClient;

        static JsonSerializerOptions jsOptions = new JsonSerializerOptions { WriteIndented = true }

        public async Task FetchAndStoreNewsArticles(PerformContext context)
        {
            httpClient = httpClientFactory.CreateClient("Bungie");
            httpClient.BaseAddress = new Uri("https://www.bungie.net");

            var currentPage = 0;

            while (true)
            {
                var newsItems = await httpClient.GetStringAsync($"/Platform/Content/Rss/NewsArticles/{currentPage}/?includebody=true");

                var rss = JsonSerializer.Deserialize<BungieNewsRssResponse>(newsItems);

                foreach (var item in rss.Response.NewsArticles)
                {
                    var newsKey = $"news/{item.Link.TrimStart('/').TrimEnd('/')}/item.json";
                    var checkForNewsFile = await FileExistsInStorage(newsKey);
                    if (!checkForNewsFile)
                    {
                        await s3Client.PutObjectAsync(new PutObjectRequest
                        {
                            BucketName = "manifest-archive",
                            Key = newsKey,
                            ContentBody = JsonSerializer.Serialize(item, jsOptions)
                        });
                    }
                }

                if (!rss.Response.NextPaginationToken.HasValue)
                {
                    break;
                }

                currentPage++;
            }
        }

        private async Task<bool> FileExistsInStorage(string path)
        {
            try
            {
                await s3Client.GetObjectMetadataAsync("manifest-archive", path);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
    }
}
