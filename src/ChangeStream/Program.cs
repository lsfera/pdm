using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ChangeStream
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            BsonClassMap.RegisterClassMap<Fescion>(_ => _.AutoMap());
            var client = new MongoClient(
                "mongodb://test:test123@mong01,mongo2,mongo3:27017/pdm?replicaSet=rs0"
            );
            var collection = client.GetDatabase("pdm").GetCollection<Fescion>("fescion");
            var streamCollection = client.GetDatabase("pdm").GetCollection<Stream<Fescion>>("stream",new MongoCollectionSettings{WriteConcern = WriteConcern.Acknowledged});

            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                MaxAwaitTime = TimeSpan.FromMinutes(1),
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
                foreach (var info in cursor.Current)
                    try
                    {
                        Interlocked.Increment(ref documents);

                        var fescion = info.FullDocument;
                        await streamCollection.UpdateOneAsync(
                            Builders<Stream<Fescion>>.Filter.Eq(d => d.Id, fescion.Id),
                            Builders<Stream<Fescion>>.Update
                                .AddToSet(_ => _.Documents,  (fescion, info.OperationType.ToString())),
                        options: new UpdateOptions
                            {
                                IsUpsert = true,
                            },
                            token).ConfigureAwait(false);
                        Console.WriteLine($"{info.BackingDocument}");
                        Console.WriteLine($"Processed {documents} documetns");

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
        }
    }
}

public  record   Material(String Id);
public  record  Fescion(String Id, String C10, String[] Gtin, Material[] Materials);
public record Stream<T>(String Id, (T, String Operation)[] Documents) where T:class;