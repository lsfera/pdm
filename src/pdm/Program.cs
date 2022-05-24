using System.Text.Json;
using MongoDB.Driver;

namespace PDM
{
    internal class Program
    {
        static Task Main(string[] args)
        {
            var client = new MongoClient(
                "mongodb://test:test123@mong01,mongo2,mongo3:27017/pdm?replicaSet=rs0"
            );
            var collection = client.GetDatabase("pdm").GetCollection<Fescion>("fescion",
                new MongoCollectionSettings
                    { ReadConcern = ReadConcern.Snapshot, WriteConcern = WriteConcern.WMajority });
            var streamCollection = client.GetDatabase("pdm").GetCollection<Stream<Fescion>>("stream",
                new MongoCollectionSettings { WriteConcern = WriteConcern.WMajority });
            var token = CancellationToken.None;


            var ops = Enumerable.Range(1, 100).Select(i =>
            {

                var msg = new Fescion(
                    id: "c8",
                    c10: "c10",
                    gtin: new List<String> { $"gtin{i}" },
                    materials: new[] { new Material(id: "mat") },
                    streamId: Guid.NewGuid()
                );
                try
                {


                    return collection.FindOneAndUpdateAsync(
                            Builders<Fescion>.Filter.Eq(d => d.id, msg.id),
                                Builders<Fescion>.Update
                                .AddToSetEach(_ => _.gtin, msg.gtin)
                                .Set(_ => _.streamId, msg.streamId)
                                .Set(_ => _.c10, msg.c10)
                                .AddToSetEach(_ => _.materials, msg.materials),
                    options: new FindOneAndUpdateOptions<Fescion>
                            {
                                ReturnDocument = ReturnDocument.Before,
                                IsUpsert = true,
                            }, token)
                        .ContinueWith(ta =>
                        {
                            Console.WriteLine(ta.Status);
                            if (ta.Exception != null)
                                Console.WriteLine(ta.Exception.GetBaseException());

                            var info = ta.Result;

                            if (info == null)
                                return Task.CompletedTask;

                            Console.WriteLine($"{JsonSerializer.Serialize(info)}");


                            return streamCollection.UpdateOneAsync(
                                Builders<Stream<Fescion>>.Filter.Eq(d => d.id, info.streamId),
                                Builders<Stream<Fescion>>.Update
                                    .Set(_ => _.originalDocument, info),
                                options: new UpdateOptions
                                {
                                    IsUpsert = true,
                                },

                                token);


                            //var before = JsonNode.Parse(JsonSerializer.Serialize(b));
                            //var current = JsonNode.Parse(JsonSerializer.Serialize(a));
                            //var diff = before.Diff(current, new JsonPatchDeltaFormatter());


                            //Console.WriteLine(before);
                            //Console.WriteLine("current");
                            //Console.WriteLine(current);
                            //Console.WriteLine("diff");
                            //Console.WriteLine(diff);
                        }, token)
                        .Unwrap();

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);

                    throw;
                }
            });
            return Task.WhenAll(ops).ContinueWith(_ => { Console.WriteLine($"done with status:{_.Status}"); }, token);
        }
    }
}

public  record   Material(String id);
public  record  Fescion(String id, String c10, List<String> gtin, Material[] materials, Guid streamId);
public record Stream<T>(Guid id, T originalDocument) where T : class;