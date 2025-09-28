using Manifest.Report.Classes;
using Manifest.Report.Classes.DBClasses;
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

        [HttpGet("/search/name")]
        public async Task<IActionResult> SearchByName(string name, int limit = 10, bool? includeData = false)
        {
            if (string.IsNullOrWhiteSpace(name) || limit <= 0)
            {
                return BadRequest("Invalid parameters.");
            }

            if (limit > 1000)
            {
                limit = 1000; // Cap the limit to a maximum of 1000
            }

            var sql = $@"
                SELECT DISTINCT TOP {limit} *
                FROM DefinitionHashes
                WHERE DisplayName LIKE '%' + @name + '%'
                ORDER BY FirstDiscoveredUTC DESC";
            var parameters = new[]
            {
                new SqlParameter("name", name)
            };

            var results = await db.ExecuteListAsync<DestinyDefinitionHashCollectionItem>(sql, parameters);
            if (results.Count == 0)
            {
                return NotFound(new
                {
                    error = new
                    {
                        code = 404,
                        message = $"No definitions found matching '{name}'.",
                    }
                });
            }

            // Get total count to let users know how many results are available if they specify the name better
            var countSql = "SELECT COUNT(Hash) FROM DefinitionHashes WHERE DisplayName LIKE '%' + @name + '%'";
            var totalCount = await db.ExecuteScalarAsync<int>(countSql, new SqlParameter("name", name));

            return Ok(new
            {
                data = results.Select(i => new
                {
                    i.Definition,
                    i.Hash,
                    i.DisplayName,
                    i.DisplayIcon,
                    Data = includeData ?? false ? JsonSerializer.Deserialize<JsonNode>(i.JSONContent) : null
                }),
                totalCount,
                __help = "If you want to change how many results you can view, add limit as a query parameter (min 1, max 1000), and if you want to include the data, add includeData=true into the query"
            });
        }

        [HttpGet("/search/hash")]
        public async Task<IActionResult> SearchByHash(long hash, int limit = 10, bool? includeData = false)
        {
            if (hash < 0 || limit <= 0)
            {
                return BadRequest("Invalid parameters.");
            }

            if (limit > 1000)
            {
                limit = 1000; // Cap the limit to a maximum of 1000
            }

            var sql = $@"
                SELECT DISTINCT TOP {limit} *
                FROM DefinitionHashes
                WHERE Hash = @hash
                ORDER BY FirstDiscoveredUTC DESC";
            var parameters = new[]
            {
                new SqlParameter("hash", hash)
            };

            var results = await db.ExecuteListAsync<DestinyDefinitionHashCollectionItem>(sql, parameters);
            if (results.Count == 0)
            {
                return NotFound(new
                {
                    error = new
                    {
                        code = 404,
                        message = $"No hashes found matching the hash '{hash}'.",
                    }
                });
            }

            // Get total count to let users know how many results are available if they specify the name better
            var countSql = "SELECT COUNT(Hash) FROM DefinitionHashes WHERE Hash = @hash";
            var totalCount = await db.ExecuteScalarAsync<int>(countSql, new SqlParameter("hash", hash));

            return Ok(new
            {
                data = results.Select(i => new
                {
                    i.Definition,
                    i.Hash,
                    i.DisplayName,
                    i.DisplayIcon,
                    Data = includeData ?? false ? JsonSerializer.Deserialize<JsonNode>(i.JSONContent) : null
                }),
                totalCount,
                __help = "If you want to change how many results you can view, add limit as a query parameter (min 1, max 1000), and if you want to include the data, add includeData=true into the query"
            });
        }

        [HttpGet("/d1/hash/search")]
        public async Task<IActionResult> SearchD1Hash(long hash, int limit = 10)
        {
            if (hash < 0 || limit <= 0)
            {
                return BadRequest("Invalid parameters.");
            }

            if (limit > 1000)
            {
                limit = 1000; // Cap the limit to a maximum of 1000
            }

            var sql = $@"
                SELECT DISTINCT TOP {limit} *
                FROM D1DefinitionData
                WHERE Hash = @hash
                ORDER BY Definition";
            var parameters = new[]
            {
                new SqlParameter("hash", hash)
            };

            var results = await db.ExecuteListAsync<D1DefinitionData>(sql, parameters);
            if (results.Count == 0)
            {
                return NotFound(new
                {
                    error = new
                    {
                        code = 404,
                        message = $"No hashes found matching the hash '{hash}'.",
                    }
                });
            }

            // Get total count to let users know how many results are available if they specify the name better
            var countSql = "SELECT COUNT(Hash) FROM D1DefinitionData WHERE Hash = @hash";
            var totalCount = await db.ExecuteScalarAsync<int>(countSql, new SqlParameter("hash", hash));

            return Ok(new
            {
                data = results.Select(i => new
                {
                    i.Definition,
                    i.Hash,
                    i.Data
                }),
                totalCount,
                __help = "If you want to change how many results you can view, add limit as a query parameter (min 1, max 1000)"
            });
        }

        [HttpGet("/d1/name/search")]
        public async Task<IActionResult> SearchD1Name(string name, int limit = 10)
        {
            if (string.IsNullOrWhiteSpace(name) || limit <= 0)
            {
                return BadRequest("Invalid parameters.");
            }

            if (limit > 1000)
            {
                limit = 1000; // Cap the limit to a maximum of 1000
            }

            var sql = $@"
                SELECT DISTINCT TOP {limit} *
                FROM D1DefinitionData
                WHERE DisplayName LIKE '%' + @name + '%'
                ORDER BY Definition";
            var parameters = new[]
            {
                new SqlParameter("name", name)
            };

            var results = await db.ExecuteListAsync<D1DefinitionData>(sql, parameters);
            if (results.Count == 0)
            {
                return NotFound(new
                {
                    error = new
                    {
                        code = 404,
                        message = $"No definitions found matching the name '{name}'.",
                    }
                });
            }

            // Get total count to let users know how many results are available if they specify the name better
            var countSql = "SELECT COUNT(Hash) FROM D1DefinitionData WHERE DisplayName LIKE '%' + @name + '%'";
            var totalCount = await db.ExecuteScalarAsync<int>(countSql, new SqlParameter("name", name));

            return Ok(new
            {
                data = results.Select(i => new
                {
                    i.Definition,
                    i.Hash,
                    i.Data
                }),
                totalCount,
                __help = "If you want to change how many results you can view, add limit as a query parameter (min 1, max 1000)"
            });
        }
    }
}
