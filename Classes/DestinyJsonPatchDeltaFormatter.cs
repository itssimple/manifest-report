using System.Text.Json.Nodes;
using System.Text;
using System.Text.Json.JsonDiffPatch.Diffs.Formatters;
using System.Text.Json.JsonDiffPatch.Diffs;

namespace Manifest.Report.Classes
{
    public class DestinyJsonPatchDeltaFormatter : DefaultDeltaFormatter<JsonNode>
    {
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

        protected (string hash, long? longHash) GetHashAndPrepareObject(JsonNode? existingValue)
        {
            var hash = PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).First();

            if (existingValue!.AsObject()[hash] == null)
            {
                existingValue!.AsObject()[hash] = new JsonObject {
                { "diff", new JsonArray() }
            };
            }

            long.TryParse(hash, out var longHash);

            Changes.Modified.Add(longHash);
            Changes.Changes++;

            return (hash, longHash);
        }

        protected override JsonNode FormatAdded(ref JsonDiffDelta delta, JsonNode? existingValue)
        {
            var diff = new JsonObject
        {
            { "op", "add" },
            { "path", PathBuilder.ToString() },
            { "new", delta.GetAdded() }
        };

            (var hash, var longHash) = GetHashAndPrepareObject(existingValue);

            if (PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).Count() == 1)
            {
                Changes.Added.Add(longHash.Value);
                Changes.Changes++;
            }

            existingValue!.AsObject()[hash]!.AsObject()["diff"]!.AsArray().Add(diff);

            return existingValue;
        }

        protected override JsonNode FormatArrayMove(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
        {
            throw new NotImplementedException();
        }

        protected override JsonNode FormatDeleted(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
        {
            var diff = new JsonObject
        {
            { "op", "del" },
            { "path", PathBuilder.ToString() },
            { "old", delta.GetDeleted() }
        };

            (var hash, var longHash) = GetHashAndPrepareObject(existingValue);

            if (PathBuilder.ToString().Split('/', StringSplitOptions.RemoveEmptyEntries).Count() == 1)
            {
                Changes.Removed.Add(longHash.Value);
                Changes.Changes++;
            }

            existingValue!.AsObject()[hash]!.AsObject()["diff"]!.AsArray().Add(diff);

            return existingValue;
        }

        protected bool IsImageString(JsonObject diff)
        {
            var operation = diff["op"]!.AsValue().GetValue<string>();
            diff["old"]!.AsValue().TryGetValue<string>(out var oldString);
            diff["new"]!.AsValue().TryGetValue<string>(out var newString);

            List<string> imageEndings = [".png", ".jpeg", ".jpg", ".gif"];

            if (oldString is null && newString is null) return false;

            return operation == "edit" && imageEndings.Any(e => oldString.EndsWith(e)) && imageEndings.Any(e => newString.EndsWith(e));
        }

        protected override JsonNode FormatModified(ref JsonDiffDelta delta, JsonNode? left, JsonNode? existingValue)
        {
            if (IgnoredProperties.Contains(CurrentProperty))
            {
                return existingValue!;
            }

            var diff = new JsonObject
        {
            { "op", "edit" },
            { "path", PathBuilder.ToString() },
            { "old", delta.GetOldValue() },
            { "new", delta.GetNewValue() }
        };

            if (IsImageString(diff)) { return existingValue!; }

            (var hash, var longHash) = GetHashAndPrepareObject(existingValue);

            if (PathBuilder.ToString().EndsWith("/redacted"))
            {
                var redacted = delta.GetNewValue().AsValue().GetValue<bool>();

                if (!redacted)
                {
                    Changes.Unclassified.Add(longHash.Value);
                }
                else
                {
                    Changes.Reclassified.Add(longHash.Value);
                }
            }

            existingValue!.AsObject()[hash]!.AsObject()["diff"]!.AsArray().Add(diff);

            return existingValue;
        }

        protected override JsonNode FormatTextDiff(ref JsonDiffDelta delta, JsonValue? left, JsonNode? existingValue)
        {
            throw new NotImplementedException();
        }
    }
}
