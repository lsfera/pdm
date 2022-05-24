using MongoDB.Driver;

namespace ChangeStream
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            
            var client = new MongoClient(
                "mongodb://test:test123@mong01,mongo2,mongo3:27017/pdm?replicaSet=rs0"
            );
            var collection = client.GetDatabase("pdm").GetCollection<Fescion>("fescion");
            var streamCollection = client.GetDatabase("pdm").GetCollection<Stream<Fescion>>("stream");

            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                BatchSize = 1
            };

            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<Fescion>>()
                .Match(g =>
                    g.OperationType == ChangeStreamOperationType.Update ||
                    g.OperationType == ChangeStreamOperationType.Insert);
            var documents = 0;
            var matched = 0;
            var modified = 0;
            var token = CancellationToken.None;
            using var cursor = await collection.WatchAsync(pipeline, options, token).ConfigureAwait(false);
            while (await cursor.MoveNextAsync(token).ConfigureAwait(false))
            {
                foreach (var info in cursor.Current)
                {
                    try
                    {
                        Interlocked.Increment(ref documents);

                        var fescion = info.FullDocument;
                        var result = await streamCollection.UpdateOneAsync(
                            Builders<Stream<Fescion>>.Filter.Eq(d => d.id, fescion.streamId),
                            Builders<Stream <Fescion>>.Update
                                .Set(_ =>_.modifiedDocument , fescion)
                                .Set(_ =>_.operation , info.OperationType.ToString())
                                .SetOnInsert(_ =>_.id , fescion.streamId),
                            options: new UpdateOptions
                            {
                                IsUpsert = true,
                            },
                            token).ConfigureAwait(false);
                        if (result.MatchedCount > 0) Interlocked.Increment(ref matched);
                        if (result.ModifiedCount > 0) Interlocked.Increment(ref modified);

                        Console.WriteLine($"docs:{documents}, matched: {matched}, modified: {modified} ");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }
        }
    }
}

public  record   Material(String id);
public  record  Fescion(String id, String c10, List<String> gtin, Material[] materials, Guid streamId);
public record Stream<T>(Guid id, T modifiedDocument, String operation) where T:class;