using Amazon.S3.Model;
using Manifest.Report.Classes;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json.JsonDiffPatch.Diffs.Formatters;
using System.Text.Json.JsonDiffPatch.Diffs;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Text;
using Amazon.S3;
using Manifest.Report.Classes.DBClasses;
using System.Text.Json.JsonDiffPatch;
using Hangfire.Server;
using Hangfire.Console;

namespace Manifest.Report
{
    public class StoreManifestDiffs(ILogger<StoreManifestDiffs> logger, AmazonS3Client s3Client, MSSQLDB db, IServiceProvider serviceProvider)
    {
        static HashSet<string> DoneItems = new HashSet<string>();

        static ILogger<StoreManifestDiffs> Logger = null;
        static PerformContext _context;

        static void LogInformation(string message)
        {
            Logger?.LogInformation(message);
            _context?.WriteLine(message);
        }

        public async Task StoreDiffs(PerformContext context)
        {
            Logger = logger;
            _context = context;

            LogInformation("Starting to store manifest diffs...");

            var list = await s3Client.GetObjectAsync("manifest-archive", "list.json");

            using var sr = new StreamReader(list.ResponseStream);

            var listJson = await sr.ReadToEndAsync();

            var manifestList = JsonSerializer.Deserialize<List<ManifestInfo>>(listJson).OrderBy(m => m.DiscoverDate_UTC).ToList();

            Thread saveThread = new Thread(new ThreadStart(SaveFromQueue));
            saveThread.Start();

            var sql = "SELECT MAX(DiscoveredUTC) FROM DefinitionHashHistory";
            var lastDiscoveredUTC = await db.ExecuteScalarAsync<DateTimeOffset>(sql);

            LogInformation($"Last discovered UTC: {lastDiscoveredUTC}");

            sql = @"SELECT ManifestVersion, Definition, Hash
        FROM DefinitionHashHistory
        WHERE DiscoveredUTC >= @maxDiscovered
        ORDER BY ManifestVersion, Definition";

            await using var reader = await db.ExecuteReader(sql, new SqlParameter("maxDiscovered", lastDiscoveredUTC));
            while (await reader.ReadAsync())
            {
                var version = reader.GetGuid(0);
                var definition = reader.GetString(1);
                var hash = reader.GetInt64(2);

                DoneItems.Add($"{version}|{definition}|{hash}");
            }

            await reader.CloseAsync();

            if (lastDiscoveredUTC <= manifestList[0].DiscoverDate_UTC)
            {
                LogInformation($"Processing first version: {manifestList[0].VersionId} with discover date {manifestList[0].DiscoverDate_UTC}");

                var firstVersionFiles = await FetchAllVersionFiles(manifestList[0].VersionId);

                var firstAddedFiles = FindNewFiles([], firstVersionFiles, "versions/dummy", $"versions/{manifestList[0].VersionId}");

                var firstChanges = await FindDiffsBetweenFiles(null, manifestList[0], [], [], firstVersionFiles, firstAddedFiles, []);
            }

            for (int i = 1; i < manifestList.Count; i++)
            {
                var oldVersion = manifestList[i - 1];
                var newVersion = manifestList[i];

                var oldMan = oldVersion.VersionId;
                var newMan = manifestList[i].VersionId;

                if (lastDiscoveredUTC <= newVersion.DiscoverDate_UTC)
                {
                    LogInformation($"Processing version: {newVersion.VersionId} with discover date {newVersion.DiscoverDate_UTC}");

                    var oldFiles = await FetchAllVersionFiles(oldMan);
                    var newFiles = await FetchAllVersionFiles(newMan);

                    var addedFiles = FindNewFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");
                    var removedFiles = FindRemovedFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");
                    var modifiedFiles = FindModifiedFiles(oldFiles, newFiles, $"versions/{oldMan}", $"versions/{newMan}");

                    var changes = await FindDiffsBetweenFiles(oldVersion, newVersion, modifiedFiles, oldFiles, newFiles, addedFiles, removedFiles);

                    newVersion.DiffFiles = changes;

                    manifestList[i] = newVersion;

                    lastDiscoveredUTC = newVersion.DiscoverDate_UTC;
                }
            }

            SaveBreak = true;
            LogInformation("Waiting for save thread to finish...");
            saveThread.Join(TimeSpan.FromSeconds(30));
            LogInformation("Save thread finished.");
        }

        async Task<List<S3Object>> FetchAllVersionFiles(Guid version)
        {
            ListObjectsV2Response fileResp;
            var request = new ListObjectsV2Request
            {
                BucketName = "manifest-archive",
                Prefix = $"/versions/{version}/tables"
            };

            List<S3Object> files = new List<S3Object>();

            do
            {
                fileResp = await s3Client.ListObjectsV2Async(request);
                files.AddRange(fileResp.S3Objects.Where(f => !f.Key.Contains("/diffFiles/")));

                request.ContinuationToken = fileResp.NextContinuationToken;
            } while (fileResp.IsTruncated);

            return files;
        }

        public static DestinyDefinitionHashCollectionItem GetOrCreateItem(MSSQLDB db, string Definition, long Hash)
        {
            var existingItem = db.ExecuteSingleRow<DestinyDefinitionHashCollectionItem>(
                "SELECT * FROM DefinitionHashes WHERE Definition = @definition AND Hash = @hash",
                new SqlParameter("definition", Definition),
                new SqlParameter("hash", Hash)
            );

            if (existingItem != null) return existingItem;

            existingItem = new DestinyDefinitionHashCollectionItem
            {
                Definition = Definition,
                Hash = Hash,
                IsDirty = true
            };

            return existingItem;
        }

        public static DestinyDefinitionHashHistoryCollectionItem GetOrCreateHistoryItem(MSSQLDB db, Guid versionId, string Definition, long Hash)
        {
            var existingItem = db.ExecuteSingleRow<DestinyDefinitionHashHistoryCollectionItem>(
                "SELECT * FROM DefinitionHashHistory WHERE ManifestVersion = @version AND Definition = @definition AND Hash = @hash",
                new SqlParameter("version", versionId),
                new SqlParameter("definition", Definition),
                new SqlParameter("hash", Hash)
            );

            if (existingItem != null) return existingItem;

            existingItem = new DestinyDefinitionHashHistoryCollectionItem
            {
                Definition = Definition,
                Hash = Hash,
                ManifestVersion = versionId,
                IsDirty = true
            };

            return existingItem;
        }

        public static Queue<DestinyDefinitionHashCollectionItem> SaveItems = new Queue<DestinyDefinitionHashCollectionItem>();
        public static Queue<DestinyDefinitionHashHistoryCollectionItem> SaveHistoryItems = new Queue<DestinyDefinitionHashHistoryCollectionItem>();
        public static bool SaveBreak = false;

        public static void SaveItem(DestinyDefinitionHashCollectionItem item)
        {
            if (item.IsDirty) SaveItems.Enqueue(item);

            if (SaveItems.Count > 500)
            {
                while (SaveItems.Count > 0)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public void SaveHistoryItem(DestinyDefinitionHashHistoryCollectionItem item)
        {
            if (item.IsDirty) SaveHistoryItems.Enqueue(item);

            if (SaveHistoryItems.Count > 500)
            {
                while (SaveHistoryItems.Count > 0)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public void SaveFromQueue()
        {
            using (var db = serviceProvider.CreateScope().ServiceProvider.GetRequiredService<MSSQLDB>())
            {
                while (!SaveBreak)
                {
                    while (SaveItems.Count > 0)
                    {
                        if (SaveItems.TryDequeue(out var _item))
                        {
                            string sql = string.Empty;
                            if (_item.HashCollectionId == 0)
                            {
                                sql = @"INSERT INTO DefinitionHashes (Definition, Hash, FirstDiscoveredUTC, RemovedUTC, DisplayName, DisplayIcon, InVersions, JSONContent)
VALUES
(@Definition, @Hash, @FirstDiscoveredUTC, @RemovedUTC, @DisplayName, @DisplayIcon, @InVersions, @JSONContent);";
                            }
                            else
                            {
                                sql = @"UPDATE DefinitionHashes
SET FirstDiscoveredUTC = @FirstDiscoveredUTC,
RemovedUTC = @RemovedUTC,
DisplayName = @DisplayName,
DisplayIcon = @DisplayIcon,
InVersions = @InVersions,
JSONContent = @JSONContent
WHERE Definition = @Definition
AND Hash = @Hash
AND HashCollectionId = @HashCollectionId";
                            }

                            db.ExecuteNonQuery(sql,
                                new SqlParameter("HashCollectionId", _item.HashCollectionId),
                                new SqlParameter("Definition", _item.Definition),
                                new SqlParameter("Hash", _item.Hash),
                                new SqlParameter("FirstDiscoveredUTC", _item.FirstDiscoveredUTC ?? DateTimeOffset.MinValue),
                                _item.RemovedUTC.HasValue ?
                                    new SqlParameter("RemovedUTC", _item.RemovedUTC) :
                                    new SqlParameter("RemovedUTC", SqlDbType.DateTimeOffset) { IsNullable = true, SqlValue = DBNull.Value },
                                !string.IsNullOrWhiteSpace(_item.DisplayName) ?
                                    new SqlParameter("DisplayName", _item.DisplayName) :
                                    new SqlParameter("DisplayName", SqlDbType.NVarChar) { IsNullable = true, SqlValue = DBNull.Value },
                                !string.IsNullOrWhiteSpace(_item.DisplayIcon) ?
                                    new SqlParameter("DisplayIcon", _item.DisplayIcon) :
                                    new SqlParameter("DisplayIcon", SqlDbType.NVarChar) { IsNullable = true, SqlValue = DBNull.Value },
                                new SqlParameter("InVersions", JsonSerializer.Serialize(_item.InVersions)),
                                !string.IsNullOrWhiteSpace(_item.JSONContent) ?
                                    new SqlParameter("JSONContent", _item.JSONContent) :
                                    new SqlParameter("JSONContent", SqlDbType.NVarChar) { IsNullable = true, SqlValue = DBNull.Value }
                            );
                        }
                    }

                    while (SaveHistoryItems.Count > 0)
                    {
                        if (SaveHistoryItems.TryDequeue(out var _item))
                        {
                            string sql = string.Empty;
                            if (_item.HistoryId == 0)
                            {
                                sql = @"INSERT INTO DefinitionHashHistory (Definition, Hash, ManifestVersion, DiscoveredUTC, JSONContent, JSONDiff, State)
VALUES
(@Definition, @Hash, @ManifestVersion, @DiscoveredUTC, @JSONContent, @JSONDiff, @State);";
                            }
                            else
                            {
                                sql = @"UPDATE DefinitionHashHistory
SET DiscoveredUTC = @DiscoveredUTC,
JSONContent = @JSONContent,
JSONDiff = @JSONDiff,
State = @State
WHERE Definition = @Definition
AND Hash = @Hash
AND HistoryId = @HistoryId";
                            }

                            db.ExecuteNonQuery(sql,
                                new SqlParameter("HistoryId", _item.HistoryId),
                                new SqlParameter("Definition", _item.Definition),
                                new SqlParameter("Hash", _item.Hash),
                                new SqlParameter("ManifestVersion", _item.ManifestVersion),
                                new SqlParameter("DiscoveredUTC", _item.DiscoveredUTC),
                                !string.IsNullOrWhiteSpace(_item.JSONContent) ?
                                    new SqlParameter("JSONContent", _item.JSONContent) :
                                    new SqlParameter("JSONContent", SqlDbType.NVarChar) { IsNullable = true, SqlValue = DBNull.Value },
                                !string.IsNullOrWhiteSpace(_item.JSONDiff) ?
                                    new SqlParameter("JSONDiff", _item.JSONDiff) :
                                    new SqlParameter("JSONDiff", SqlDbType.NVarChar) { IsNullable = true, SqlValue = DBNull.Value },
                                new SqlParameter("State", _item.State.ToString())
                            );
                        }
                    }

                    Thread.Sleep(5000);
                }
            }
        }

        public enum FileStatus
        {
            Added,
            Modified,
            Removed
        }

        DestinyJsonPatchDeltaFormatter formatter = new DestinyJsonPatchDeltaFormatter();

        public async Task<List<FileDiff>> FindDiffsBetweenFiles(ManifestInfo oldVersion, ManifestInfo newVersion, List<string> diffFiles, List<S3Object> previousFiles, List<S3Object> currentFiles, List<string> addedFiles, List<string> removedFiles)
        {
            List<FileDiff> changes = new List<FileDiff>();

            async Task WriteDiffFile(string diffFile, FileStatus fileStatus, JsonNode? prevJson, JsonNode? currJson)
            {
                formatter.Database = db;
                formatter.Definition = CleanDefinitionName(diffFile);
                formatter.ManifestInfo = newVersion;
                formatter.PreviousManifestInfo = oldVersion;

                formatter.PreviousDocument = prevJson;
                formatter.CurrentDocument = currJson;
                var diff = prevJson.Diff(currJson, formatter)!;

                if (formatter.Changes.Changes > 0)
                {
                    foreach (var prop in diff.AsObject())
                    {
                        if (DoneItems.Contains($"{newVersion.VersionId}|{formatter.Definition}|{prop.Key}"))
                        {
                            continue;
                        }

                        long.TryParse(prop.Key, out var longHash);

                        var hashObject = prop.Value.AsObject();
                        if (hashObject["diff"] != null &&
                            hashObject["diff"].AsArray().Count > 0)
                        {
                            DefinitionHashChangeState itemState = DefinitionHashChangeState.Modified;

                            var historyItem = GetOrCreateHistoryItem(db, newVersion.VersionId, formatter.Definition, longHash);
                            if (historyItem.DiscoveredUTC != newVersion.DiscoverDate_UTC)
                            {
                                historyItem.DiscoveredUTC = newVersion.DiscoverDate_UTC;
                                historyItem.IsDirty = true;
                            }

                            if (historyItem.JSONDiff != hashObject["diff"].ToJsonString())
                            {
                                historyItem.JSONDiff = hashObject["diff"].ToJsonString();
                                historyItem.IsDirty = true;
                            }

                            foreach (var change in hashObject["diff"].AsArray())
                            {
                                change["op"].AsValue().TryGetValue<string>(out var operation);
                                change["path"].AsValue().TryGetValue<string>(out var path);

                                var isRootObject = path.Split('/', StringSplitOptions.RemoveEmptyEntries).Length == 1;
                                switch (operation)
                                {
                                    case "add":
                                        if (isRootObject)
                                        {
                                            itemState = DefinitionHashChangeState.Added;
                                            if (historyItem.JSONContent != change["new"].ToJsonString())
                                            {
                                                historyItem.JSONContent = change["new"].ToJsonString();
                                                historyItem.IsDirty = true;
                                            }
                                        }
                                        break;
                                    case "del":
                                        if (isRootObject)
                                        {
                                            itemState = DefinitionHashChangeState.Deleted;
                                            if (historyItem.JSONContent != change["old"].ToJsonString())
                                            {
                                                historyItem.JSONContent = change["old"].ToJsonString();
                                                historyItem.IsDirty = true;
                                            }
                                        }
                                        break;
                                    case "edit":
                                        if (path.EndsWith("/redacted"))
                                        {
                                            change["new"].AsValue().TryGetValue<bool>(out var redactedValue);
                                            if (redactedValue)
                                            {
                                                itemState = DefinitionHashChangeState.Redacted;
                                            }
                                            else
                                            {
                                                itemState = DefinitionHashChangeState.Unredacted;
                                            }
                                        }
                                        break;
                                }
                            }

                            if (historyItem.State != itemState)
                            {
                                historyItem.State = itemState;
                                historyItem.IsDirty = true;
                            }

                            if (currJson.AsObject()[prop.Key] != null && historyItem.JSONContent != currJson.AsObject()[prop.Key].ToJsonString())
                            {
                                historyItem.JSONContent = currJson.AsObject()[prop.Key].ToJsonString();
                                historyItem.IsDirty = true;
                            }
                            else
                            {
                                if (prevJson.AsObject()[prop.Key] != null && historyItem.JSONContent != prevJson.AsObject()[prop.Key].ToJsonString())
                                {
                                    historyItem.JSONContent = prevJson.AsObject()[prop.Key].ToJsonString();
                                    historyItem.IsDirty = true;
                                }
                            }

                            SaveHistoryItem(historyItem);
                        }
                    }
                }

                LogInformation($"Found {formatter.Changes.Changes} changes in {diffFile} for {formatter.Definition}");

                while (SaveItems.Count > 0)
                {
                    Thread.Sleep(100);
                }

                while (SaveHistoryItems.Count > 0)
                {
                    Thread.Sleep(100);
                }

                await Task.CompletedTask;
            }

            foreach (var diffFile in diffFiles)
            {
                var prevJson = await GetFileAsJsonNode(diffFile, previousFiles);
                var currJson = await GetFileAsJsonNode(diffFile, currentFiles);

                await WriteDiffFile(diffFile, FileStatus.Modified, prevJson, currJson);
            }

            foreach (var added in addedFiles)
            {
                var prevJson = new JsonObject();
                var currJson = await GetFileAsJsonNode(added, currentFiles);

                await WriteDiffFile(added, FileStatus.Added, prevJson, currJson);
            }

            foreach (var removed in removedFiles)
            {
                var prevJson = await GetFileAsJsonNode(removed, previousFiles);
                var currJson = new JsonObject();

                await WriteDiffFile(removed, FileStatus.Removed, prevJson, currJson);
            }

            return changes;
        }

        async Task<JsonNode> GetFileAsJsonNode(string diffFile, List<S3Object> files)
        {
            var file = files.FirstOrDefault(f => f.Key.EndsWith(diffFile));
            var fileFetch = await s3Client.GetObjectAsync(file.BucketName, file.Key);
            using var sr = new StreamReader(fileFetch.ResponseStream);
            string json = sr.ReadToEnd();

            var node = JsonSerializer.Deserialize<JsonNode>(json);
            return node;
        }

        static string CleanDefinitionName(string diffFile)
        {
            return diffFile
                .Replace("/tables/Destiny", string.Empty)
                .Replace("Definition.json", string.Empty);
        }

        public class DestinyJsonPatchDeltaFormatter : DefaultDeltaFormatter<JsonNode>
        {
            public string Definition { get; set; }
            public ManifestInfo ManifestInfo { get; set; }
            public ManifestInfo PreviousManifestInfo { get; set; }
            public JsonNode PreviousDocument { get; set; }
            public JsonNode CurrentDocument { get; set; }

            private readonly struct PropertyPathScope : IDisposable
            {
                private readonly StringBuilder _pathBuilder;

                private readonly int _startIndex;

                private readonly int _length;

                public PropertyPathScope(StringBuilder pathBuilder, string propertyName)
                {
                    _pathBuilder = pathBuilder;
                    _startIndex = pathBuilder.Length;
                    pathBuilder.Append('/');
                    pathBuilder.Append(Escape(propertyName));
                    _length = pathBuilder.Length - _startIndex;
                }

                public PropertyPathScope(StringBuilder pathBuilder, int index)
                {
                    _pathBuilder = pathBuilder;
                    _startIndex = pathBuilder.Length;
                    pathBuilder.Append('/');
                    pathBuilder.Append(index.ToString("D"));
                    _length = pathBuilder.Length - _startIndex;
                }

                public void Dispose()
                {
                    _pathBuilder.Remove(_startIndex, _length);
                }

                private static string Escape(string str)
                {
                    StringBuilder stringBuilder = new StringBuilder(str);
                    for (int i = 0; i < stringBuilder.Length; i++)
                    {
                        if (stringBuilder[i] == '/')
                        {
                            stringBuilder.Insert(i, '~');
                            stringBuilder[++i] = '1';
                        }
                        else if (stringBuilder[i] == '~')
                        {
                            stringBuilder.Insert(i, '~');
                            stringBuilder[++i] = '0';
                        }
                    }
                    return stringBuilder.ToString();
                }
            }

            protected StringBuilder PathBuilder { get; }

            protected string CurrentProperty { get; private set; }

            public DestinyDefinitionChanges Changes { get; private set; }
            public MSSQLDB Database { get; internal set; }

            public DestinyJsonPatchDeltaFormatter() : base(usePatchableArrayChangeEnumerable: true)
            {
                PathBuilder = new StringBuilder();
                Changes = new DestinyDefinitionChanges();
            }

            protected HashSet<string> IgnoredProperties = new HashSet<string> {
        "index"
    };

            protected override JsonNode? CreateDefault()
            {
                return new JsonObject();
            }

            protected override JsonNode? FormatArrayElement(in JsonDiffDelta.ArrayChangeEntry arrayChange, JsonNode? left, JsonNode? existingValue)
            {
                using (new PropertyPathScope(PathBuilder, arrayChange.Index))
                {
                    return base.FormatArrayElement(in arrayChange, left, existingValue);
                }
            }

            protected override JsonNode? FormatObjectProperty(ref JsonDiffDelta delta, JsonNode? left, string propertyName, JsonNode? existingValue)
            {
                using (new PropertyPathScope(PathBuilder, propertyName))
                {
                    CurrentProperty = propertyName;
                    return base.FormatObjectProperty(ref delta, left, propertyName, existingValue);
                }
            }

            protected (string hash, long? longHash, DestinyDefinitionHashCollectionItem dbItem) GetHashAndPrepareObject(JsonNode? existingValue)
            {
                var hash = PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();

                long.TryParse(hash, out var longHash);

                if (existingValue!.AsObject()[hash] == null)
                {
                    existingValue!.AsObject()[hash] = new JsonObject {
                        { "diff", new JsonArray() }
                    };
                }

                if (DoneItems.Contains($"{ManifestInfo.VersionId}|{Definition}|{hash}"))
                {
                    return (hash, longHash, null);
                }

                var modifiedItem = GetOrCreateItem(Database, Definition, longHash);

                if (modifiedItem.FirstDiscoveredUTC == DateTimeOffset.MinValue && PreviousManifestInfo != null)
                {
                    modifiedItem.FirstDiscoveredUTC = PreviousManifestInfo.ManifestDate_UTC;
                    modifiedItem.InVersions.Add(PreviousManifestInfo.VersionId);
                    modifiedItem.IsDirty = true;
                }
                else if (modifiedItem.FirstDiscoveredUTC == DateTimeOffset.MinValue && PreviousManifestInfo == null)
                {
                    modifiedItem.FirstDiscoveredUTC = ManifestInfo.ManifestDate_UTC;
                    modifiedItem.IsDirty = true;
                }

                if (!modifiedItem.InVersions.Contains(ManifestInfo.VersionId))
                {
                    modifiedItem.IsDirty = true;
                    modifiedItem.InVersions.Add(ManifestInfo.VersionId);
                }

                return (hash, longHash, modifiedItem);
            }

            protected override JsonNode FormatAdded(ref JsonDiffDelta delta, JsonNode? existingValue)
            {
                (var hash, var longHash, var addedItem) = GetHashAndPrepareObject(existingValue);

                var diff = new JsonObject
                {
                    { "op", "add" },
                    { "path", PathBuilder.ToString() },
                    { "new", delta.GetAdded() }
                };

                if (addedItem == null)
                {
                    existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                    return existingValue;
                }

                if (PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).Count() == 1)
                {
                    Changes.Changes++;

                    var addedNode = delta.GetAdded();

                    if (addedNode["displayProperties"] != null)
                    {
                        if (addedNode["displayProperties"]["name"] != null)
                        {
                            var displayName = addedNode["displayProperties"]["name"].GetValue<string>();

                            if (!string.IsNullOrWhiteSpace(displayName) && displayName != addedItem.DisplayName)
                            {
                                addedItem.IsDirty = true;
                                addedItem.DisplayName = displayName;
                            }
                        }

                        if (addedNode["displayProperties"]["icon"] != null)
                        {
                            var displayIcon = addedNode["displayProperties"]["icon"].GetValue<string>();

                            if (!string.IsNullOrWhiteSpace(displayIcon) && displayIcon != addedItem.DisplayIcon)
                            {
                                addedItem.IsDirty = true;
                                addedItem.DisplayIcon = displayIcon;
                            }
                        }
                    }

                    if (addedItem.JSONContent != addedNode.ToJsonString())
                    {
                        addedItem.IsDirty = true;
                        addedItem.JSONContent = addedNode.ToJsonString();
                    }

                    if (addedItem.FirstDiscoveredUTC != ManifestInfo.ManifestDate_UTC)
                    {
                        addedItem.IsDirty = true;
                        addedItem.FirstDiscoveredUTC = ManifestInfo.ManifestDate_UTC;
                    }

                    if (!addedItem.InVersions.Contains(ManifestInfo.VersionId))
                    {
                        addedItem.IsDirty = true;
                        addedItem.InVersions.Add(ManifestInfo.VersionId);
                    }

                    SaveItem(addedItem);
                }

                existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                return existingValue;
            }

            protected override JsonNode FormatArrayMove(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
            {
                throw new NotImplementedException();
            }

            protected override JsonNode FormatDeleted(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
            {
                (var hash, var longHash, var removedItem) = GetHashAndPrepareObject(existingValue);

                var diff = new JsonObject
                {
                    { "op", "del" },
                    { "path", PathBuilder.ToString() },
                    { "old", delta.GetDeleted() }
                };

                if (removedItem == null)
                {
                    existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                    return existingValue;
                }

                if (PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).Count() == 1)
                {
                    Changes.Changes++;

                    if (removedItem.JSONContent != delta.GetDeleted().ToJsonString())
                    {
                        removedItem.IsDirty = true;
                        removedItem.JSONContent = delta.GetDeleted().ToJsonString();
                    }

                    if (removedItem.FirstDiscoveredUTC == DateTimeOffset.MinValue)
                    {
                        removedItem.IsDirty = true;
                        removedItem.FirstDiscoveredUTC = PreviousManifestInfo.ManifestDate_UTC;
                        removedItem.InVersions.Add(PreviousManifestInfo.VersionId);
                    }
                    else if (PreviousManifestInfo == null)
                    {
                        removedItem.FirstDiscoveredUTC = ManifestInfo.ManifestDate_UTC;
                    }

                    if (removedItem.RemovedUTC != ManifestInfo.ManifestDate_UTC)
                    {
                        removedItem.IsDirty = true;
                        removedItem.RemovedUTC = ManifestInfo.ManifestDate_UTC;
                    }

                    SaveItem(removedItem);
                }

                existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                return existingValue;
            }

            protected bool IsImageString(JsonObject diff)
            {
                var operation = diff["op"].AsValue().GetValue<string>();
                diff["old"].AsValue().TryGetValue<string>(out var oldString);
                diff["new"].AsValue().TryGetValue<string>(out var newString);

                List<string> imageEndings = new List<string> { ".png", ".jpeg", ".jpg", ".gif" };

                if (oldString is null && newString is null) return false;

                return operation == "edit" && imageEndings.Any(e => oldString.EndsWith(e)) && imageEndings.Any(e => newString.EndsWith(e));
            }

            protected override JsonNode FormatModified(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
            {
                if (IgnoredProperties.Contains(CurrentProperty))
                {
                    return existingValue;
                }

                (var hash, var longHash, var modifiedItem) = GetHashAndPrepareObject(existingValue);

                JsonNode currentItem = null;

                var diff = new JsonObject
                {
                    { "op", "edit" },
                    { "path", PathBuilder.ToString() },
                    { "old", delta.GetOldValue() },
                    { "new", delta.GetNewValue() }
                };

                if (modifiedItem == null)
                {
                    existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                    return existingValue;
                }

                if (CurrentDocument != null && CurrentDocument[hash] != null)
                {
                    currentItem = CurrentDocument[hash];
                }
                else if (PreviousDocument != null && PreviousDocument[hash] != null)
                {
                    currentItem = PreviousDocument[hash];
                }

                Changes.Changes++;

                existingValue!.AsObject()[hash].AsObject()["diff"].AsArray().Add(diff);

                return existingValue;
            }

            protected override JsonNode FormatTextDiff(ref JsonDiffDelta delta, JsonValue? left, JsonNode? existingValue)
            {
                throw new NotImplementedException();
            }
        }

        public class DestinyDefinitionChanges
        {
            public int Changes { get; set; } = 0;
        }

        List<string> IgnoredFiles = new List<string> { "/done.txt", "/enhanced-manifest.json", "/manifest.json", ".content" };

        public List<string> FindNewFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix, string currPrefix)
        {
            var previousExceptIgnored = previousFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => s.Key.Replace(prevPrefix, "")).ToList();
            var currentExceptIgnored = currentFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => s.Key.Replace(currPrefix, "")).ToList();

            var filesNotFoundInPrevious = currentExceptIgnored.Except(previousExceptIgnored).ToList();

            return filesNotFoundInPrevious;
        }

        public List<string> FindRemovedFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix, string currPrefix)
        {
            var previousExceptIgnored = previousFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => s.Key.Replace(prevPrefix, "")).ToList();
            var currentExceptIgnored = currentFiles.Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => s.Key.Replace(currPrefix, "")).ToList();

            var filesRemovedInCurrent = previousExceptIgnored.Except(currentExceptIgnored).ToList();

            return filesRemovedInCurrent;
        }

        public List<string> FindModifiedFiles(List<S3Object> previousFiles, List<S3Object> currentFiles, string prevPrefix, string currPrefix)
        {
            var previousExceptIgnored = previousFiles
            .Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => new
            {
                File = s.Key.Replace(prevPrefix, ""),
                s.ETag,
                s.Size
            }).ToList();

            var currentExceptIgnored = currentFiles
            .Where(o => !IgnoredFiles.Any(s => o.Key.EndsWith(s))).Select(s => new
            {
                File = s.Key.Replace(currPrefix, ""),
                s.ETag,
                s.Size
            }).ToList();

            var joined = from curr in currentExceptIgnored
                         join prev in previousExceptIgnored on curr.File equals prev.File
                         select new
                         {
                             Curr = curr,
                             Prev = prev
                         };

            return joined/*.Where(f => f.Curr.ETag != f.Prev.ETag || f.Curr.Size != f.Prev.Size)*/.Select(s => s.Curr.File).ToList();
        }
    }
}
