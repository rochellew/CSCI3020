using MongoDB.Driver;
using MongoDB.Bson;
using CsvHelper;
using System.Globalization;

const string connectionUri = "mongodb+srv://admin:BlueSky2024@cluster0.4ldofje.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";

var settings = MongoClientSettings.FromConnectionString(connectionUri);

// Set the ServerApi field of the settings object to set the version of the Stable API on the client
settings.ServerApi = new ServerApi(ServerApiVersion.V1);

// Create a new client and connect to the server
var client = new MongoClient(settings);
var db = client.GetDatabase("sample_mflix");
var collection = db.GetCollection<BsonDocument>("movies");

// basic read 
var movies = collection.Find(new BsonDocument()).Limit(10).ToList();
foreach (var movie in movies)
{
    Console.WriteLine(movie["title"]);
}

// linq query example
var highRatedMovies = await collection.Find(m => m["imdb"]["rating"] > 9.0).ToListAsync();
foreach (var movie in highRatedMovies)
{
    Console.WriteLine($"Title: {movie["title"]}\nRating: {movie["imdb"]["rating"]}\n");
}

// search for movies that are directed by Christopher Nolan
var nolanMovies = await collection.Find(m => m["directors"].AsBsonArray.Contains("Christopher Nolan")).ToListAsync();
foreach (var movie in nolanMovies)
{
    Console.WriteLine($"Title: {movie["title"]}\nDirector: {movie["directors"][0]}\n");
}

// search for movies that Jack Nicholson acted in
var jackNicholsonMovies = await collection.Find(m => m["cast"].AsBsonArray.Contains("Jack Nicholson")).ToListAsync();
foreach (var movie in jackNicholsonMovies)
{
    Console.WriteLine($"Title: {movie["title"]}\n");
}

ExportToCSV(nolanMovies);

static void ExportToCSV(List<BsonDocument> movies)
{
    using (var writer = new StreamWriter("./movies.csv"))
    using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
    {
        csv.WriteRecords(movies.Select(m => new
        {
            Title = m["title"].AsString,
            Year = m["year"].AsInt32,
            Rating = m["imdb"]["rating"].AsDouble,
            Director = m["directors"][0].AsString
        }));
    }
}