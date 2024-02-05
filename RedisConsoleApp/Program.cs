using System.Diagnostics;
using StackExchange.Redis;

namespace RedisConsoleApp;

class Program
{
    static void Main()
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        // string redisConnectionString = "127.0.0.1:6379";
        string redisConnectionString = "ADD_REDIS_CONNECTION_STRING";

        var redis = ConnectionMultiplexer.Connect(redisConnectionString);
        var database = redis.GetDatabase();

        // Specify the pattern for keys you want to delete
        string keyPattern = "studio-page-generator:*";

        // // Get all keys matching the pattern in batches
        // var matchingKeys = GetKeysByPattern(database, keyPattern);
        //
        // if (!matchingKeys.Any())
        // {
        //     Console.WriteLine($"No keys found matching the pattern '{keyPattern}'.");
        //     return;
        // }
        //
        // Console.WriteLine($"Found {matchingKeys.Count} keys matching the pattern '{keyPattern}'.");
        
        var matchingKeysEnumerable = GetKeysEnumerableByPattern(database, keyPattern);

        var counter = 0;
        var deleteCount = 0;
        var logsDeleteCount = 0;
        var failedProcessingCount = 0;
        var recentCount = 0;
        var nonExistentKey = 0;
        var skippedCount = 0;
        var skippedLogKeys = 0;
        foreach (var key in matchingKeysEnumerable)
        {
            try
            {
                counter++;
                var keyString = key.ToString();

                if (keyString.EndsWith(":logs"))
                {
                    // We should handle this when we get to the un-logged endpoint. We can check any orphans later.
                    // Adding this to the Redis query can make it time out.
                    Console.WriteLine($"Index {counter}: skipping {key} as it is a log key");
                    skippedLogKeys++;
                    continue;
                }

                Console.WriteLine($"Index {counter}: fetching data for key {key}");

                var resultKey = keyString;
                var logKey = keyString + ":logs";

                var shouldDelete = false;

                var singleResult = database.HashGetAll(resultKey);

                if (!singleResult.Any())
                {
                    Console.WriteLine($"Index {counter}: key {key} does not exist, skipping");
                    nonExistentKey++;
                    skippedCount++;
                    continue;
                }

                var failedReason = singleResult
                    .Where(r => r.Name.ToString() == "failedReason")
                    .Select(r => r.Value.ToString())
                    .FirstOrDefault();

                if (failedReason != null)
                {
                    Console.WriteLine($"Index {counter}: key {key} failed processing, so skipping");
                    skippedCount++;
                    failedProcessingCount++;
                    continue;
                }

                var finishedOnStamp = singleResult
                    .Where(r => r.Name.ToString() == "finishedOn")
                    .Select(r => r.Value.ToString())
                    .FirstOrDefault();

                Console.WriteLine($"Index {counter}: finished on timestamp is {finishedOnStamp}");

                if (!string.IsNullOrEmpty(finishedOnStamp))
                {
                    var finishedOnStampLong = long.Parse(finishedOnStamp);
                    var finishedOnTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(finishedOnStampLong);

                    Console.WriteLine($"Index {counter}: parsed finished on timestamp is {finishedOnTimestamp}");

                    if (finishedOnTimestamp < DateTimeOffset.Now.AddDays(-7))
                    {
                        shouldDelete = true;
                    }
                    else
                    {
                        Console.WriteLine($"Index {counter}: too recent, will not delete");
                        recentCount++;
                    }
                }

                var logsResult = database.ListRange(logKey);

                if (logsResult.Any())
                {
                    Console.WriteLine($"Index {counter}: logs exist");
                }
                else
                {
                    Console.WriteLine($"Index {counter}: logs do not exist");
                }

                if (shouldDelete)
                {
                    Delete(database, resultKey, counter);
                    deleteCount++;

                    if (logsResult.Any())
                    {
                        Delete(database, logKey, counter);
                        logsDeleteCount++;
                    }
                }
                else
                {
                    Console.WriteLine($"Index {counter}: Did not delete key {key}.");
                    skippedCount++;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Index {counter}: Unable to handle key {key}, so skipping - {ex.Message}");
                skippedCount++;
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"Finished with {deleteCount} record deletes in {stopwatch.ElapsedMilliseconds}ms (plus {logsDeleteCount} log deletes). Opted not to delete {skippedCount} records ({failedProcessingCount} failed processing, {nonExistentKey} keys not found, {recentCount} recent) and skipped {skippedLogKeys} log keys.");
    }

    static IEnumerable<RedisKey> GetKeysEnumerableByPattern(IDatabase database, string pattern)
    {
        Console.WriteLine("Fetching key enumerable");
        var server = database.Multiplexer.GetServer(database.Multiplexer.GetEndPoints().First());

        var scanResult = server.Keys(database.Database, pattern: pattern, pageSize: 100, pageOffset: 0);
        return scanResult.Take(10000);
    }
    
    private static void Delete(IDatabase database, string key, int counter)
    {
        var fakeDelete = false;

        if (fakeDelete)
        {
            FakeDelete(database, key, counter);
        }
        else
        {
            ActualDelete(database, key, counter);
        }
    }

    private static void FakeDelete(IDatabase database, string key, int counter)
    {
        Console.WriteLine($"Index {counter}: Key '{key}' would have been deleted.");
    }

    static void ActualDelete(IDatabase database, string key, int counter)
    {
        database.KeyDelete(key);
        Console.WriteLine($"Index {counter}: Key '{key}' deleted.");
    }
}