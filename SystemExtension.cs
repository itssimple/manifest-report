using System.Text.Json;

namespace Manifest.Report
{
    public static class SystemExtension
    {
        public static T Clone<T>(this T source) => JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(source));
    }
}
