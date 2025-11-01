using System.Diagnostics;
using System.Text.Json;
using mm2;

internal class Program
{
    private static void Main()
    {
        var mesh1 = new O2M();
        mesh1.AppendElements([4, 2, 6, 0], [3, 0], [3, 1], [6, 3, 5, 1, 2, 4]);
        var mesh1t = mesh1.Transpose();
        var result = mesh1t * mesh1 * mesh1t * mesh1 + mesh1t;
        Console.WriteLine(result.ToString());
        Console.WriteLine(mesh1t.ToString());
        var mesh2 = new O2M();
        mesh2.AppendElement([4, 9, 8, 0, 1]);
        mesh2.AppendElement([3, 2, 0, 5]);
        mesh2.AppendElement([3, 1, 4]);
        var mesh3 = new O2M();
        mesh3.Reserve(20);
        mesh3.AppendElement([1, 2, 4]);
        mesh3.AppendElement([2, 4, 3]);
        mesh3.AppendElement([3]);
        mesh3.AppendElement([4]);
        var order = mesh3.GetTopOrder();
        var tr1 = mesh1.Transpose();
        var tr2 = mesh2.Transpose();
        var m1m1t = mesh1 * tr1;
        var m1tm1 = tr1 * mesh1;
        Console.WriteLine(JsonSerializer.Serialize(tr1));
        Console.WriteLine(JsonSerializer.Serialize(tr2));
        Console.WriteLine(JsonSerializer.Serialize(m1m1t));
        Console.WriteLine(JsonSerializer.Serialize(m1tm1));
        Console.WriteLine(JsonSerializer.Serialize(mesh1 + mesh2));
        Console.WriteLine(JsonSerializer.Serialize(mesh1 - mesh2));
        Console.WriteLine(JsonSerializer.Serialize(mesh1 * mesh2));
        Console.WriteLine(JsonSerializer.Serialize(mesh1 & mesh2));
        Console.WriteLine(JsonSerializer.Serialize(O2M.GetCliques(mesh1, tr1)));
        var s = mesh1.ToEpsString();
        File.WriteAllText("mesh1.eps", s);
        var o2m = new O2M();
        o2m.AppendElement([0, 3, 4]);
        o2m.AppendElement([1, 2, 4]);
        o2m.AppendElement([1, 3, 4]);
        var jsonString = JsonSerializer.Serialize(o2m);
        Console.WriteLine(jsonString);
        var o2m2 = new O2M(10);
        o2m2.AppendElement([0, 3, 4]);
        o2m2.AppendElement([1, 2, 4]);
        o2m2.AppendElement([1, 3, 4]);
        Console.WriteLine(o2m2);
        List<int> l1 = new([3, 4, 56]);
        List<int> l2 = new([3, 4, 56]);
        Console.WriteLine(l1.SequenceEqual(l2));
        var nel = 250;
        var mm = new O2M();
        mm.Reserve(nel * nel * nel);
        Console.WriteLine("Started");
        var maxnode = 0;
        for (var iex = 0; iex < nel; ++iex)
        for (var iey = 0; iey < nel; ++iey)
        for (var iez = 0; iez < nel; ++iez)
        {
            var nodes = new int[8];
            nodes[0] = iex + iey * (nel + 1) + iez * (int)Math.Pow(nel + 1, 2);
            nodes[1] = iex + 1 + iey * (nel + 1) + iez * (int)Math.Pow(nel + 1, 2);
            nodes[2] = iex + 1 + (iey + 1) * (nel + 1) + iez * (int)Math.Pow(nel + 1, 2);
            nodes[3] = iex + (iey + 1) * (nel + 1) + iez * (int)Math.Pow(nel + 1, 2);
            nodes[4] = iex + iey * (nel + 1) + (iez + 1) * (int)Math.Pow(nel + 1, 2);
            nodes[5] = iex + 1 + iey * (nel + 1) + (iez + 1) * (int)Math.Pow(nel + 1, 2);
            nodes[6] = iex + 1 + (iey + 1) * (nel + 1) + (iez + 1) * (int)Math.Pow(nel + 1, 2);
            nodes[7] = iex + (iey + 1) * (nel + 1) + (iez + 1) * (int)Math.Pow(nel + 1, 2);
            maxnode = int.Max(nodes.Max(), maxnode);
            mm.AppendElement(nodes.ToList());
        }

        Console.WriteLine($"maximum node={maxnode}\n");
        var fileName = "WeatherForecast.json";
        jsonString = JsonSerializer.Serialize(mm);
        File.WriteAllText(fileName, jsonString);
        Console.WriteLine("Finished inserting");
        var stopwatch = Stopwatch.StartNew();
        var mmt = mm.Transpose();
        Console.WriteLine("Finished Transposing");
        var mmt2 = mm * mmt;
        Console.WriteLine("Finished Multiplying");
        stopwatch.Stop();
        Console.WriteLine($"Execution Time: {stopwatch.Elapsed}");


        Console.WriteLine("DOME");
        Console.WriteLine("0\n");


        Console.WriteLine("nel-2\n");


        Console.WriteLine();
    }
}