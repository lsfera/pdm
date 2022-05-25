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
            var token = CancellationToken.None;


            var ops = Enumerable.Range(1, 10).Select(i  =>
            {

                var msg = new Fescion(
                    Id: "c8",
                    C10: "c10",
                    Gtin: new List<String> { $"gtin{i}" },
                    Materials: new[] { new Material(Id: "mat") }
                );
                return collection.UpdateOneAsync(
                    Builders<Fescion>.Filter.Eq(d => d.Id, msg.Id),
                    Builders<Fescion>.Update
                        .AddToSetEach(_ => _.Gtin, msg.Gtin)
                        .AddToSetEach(_ => _.Materials, msg.Materials)
                        .Set(_ => _.C10, msg.C10),
                    options: new UpdateOptions
                    {
                        IsUpsert = true,
                    }, token);


            });
            return Task.WhenAll(ops).ContinueWith(_ => { Console.WriteLine($"done with status:{_.Status}"); }, token);
        }
    }
}

public  record   Material(String Id);
public  record  Fescion(String Id, String C10, List<String> Gtin, Material[] Materials);

//var before = JsonNode.Parse(JsonSerializer.Serialize(b));
//var current = JsonNode.Parse(JsonSerializer.Serialize(a));
//var diff = before.Diff(current, new JsonPatchDeltaFormatter());


//Console.WriteLine(before);
//Console.WriteLine("current");
//Console.WriteLine(current);
//Console.WriteLine("diff");
//Console.WriteLine(diff);