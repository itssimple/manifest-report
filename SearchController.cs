using Manifest.Report.Classes;
using Manifest.Report.Classes.DBClasses;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Manifest.Report
{
    [Route("api/[controller]")]
    [ApiController]
    public class SearchController(MSSQLDB db, IConnectionMultiplexer connectionMultiplexer) : ControllerBase
    {
        [HttpGet("/definition/{definition}/{hash}")]
        public async Task<IActionResult> GetDefinition(string definition, long hash, bool? includeHistory = false)
        {
            if (string.IsNullOrWhiteSpace(definition) || hash <= 0)
            {
                return BadRequest("Invalid parameters.");
            }

            // Check if the definition exists in the cache
            var cacheKey = $"definition:{definition}:{hash}:{includeHistory ?? false}";
            var cache = connectionMultiplexer.GetDatabase();
            var cachedJson = await cache.StringGetAsync(cacheKey);
            if (cachedJson.HasValue && !string.IsNullOrWhiteSpace(cachedJson))
            {
                var cachedJsonContent = JsonSerializer.Deserialize<JsonNode>(cachedJson!);
                return Ok(cachedJsonContent);
            }

            var existingItem = await db.ExecuteSingleRowAsync<DestinyDefinitionHashCollectionItem>(
                "SELECT * FROM DefinitionHashes WHERE Definition = @definition AND Hash = @hash",
                new SqlParameter("definition", definition),
                new SqlParameter("hash", hash)
            );

            if (existingItem == null)
            {
                return NotFound(new
                {
                    error = new
                    {
                        code = 404,
                        message = $"Definition '{definition}' with hash '{hash}' not found in our database.",
                    }
                });
            }

            var jsonContent = JsonSerializer.Deserialize<JsonNode>(existingItem.JSONContent);

            Dictionary<string, object> jsonObject = new()
            {
                ["data"] = jsonContent!
            };

            if (jsonContent != null && includeHistory.HasValue && includeHistory.Value)
            {
                var historyItems = await db.ExecuteListAsync<DestinyDefinitionHashHistoryCollectionItem>(
                    "SELECT * FROM DefinitionHashHistory WHERE Definition = @definition AND Hash = @hash ORDER BY DiscoveredUTC ASC",
                    new SqlParameter("definition", definition),
                    new SqlParameter("hash", hash)
                );

                jsonObject["history"] = historyItems;
            }

            await cache.StringSetAsync(cacheKey, JsonSerializer.Serialize(jsonObject), TimeSpan.FromHours(1));

            return Ok(jsonObject);
        }
    }
}
