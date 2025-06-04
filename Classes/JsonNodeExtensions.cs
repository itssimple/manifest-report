using System.Text.Json.Nodes;

namespace Manifest.Report.Classes;

public static class JsonNodeExt
{
    public static IEnumerable<JsonNode> Descendants(this JsonNode node, Predicate<JsonNode> searchFunction = null)
    {
        if (node is JsonObject obj)
        {
            foreach (var property in obj)
            {
                if (searchFunction(property.Value))
                {
                    yield return property.Value;
                }
                foreach (var child in property.Value.Descendants(searchFunction))
                {
                    if (searchFunction(child))
                    {
                        yield return child;
                    }
                }
            }
        }
        else if (node is JsonArray array)
        {
            foreach (var element in array)
            {
                if (searchFunction(element))
                {
                    yield return element;
                }
                foreach (var child in element.Descendants(searchFunction))
                {
                    if (searchFunction(child))
                    {
                        yield return child;
                    }
                }
            }
        }
        else
        {
            yield break;
        }
    }
}