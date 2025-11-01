namespace mm2;

public static class Utils
{
    public static List<T> This<T>(this List<T> list, List<int> indices)
    {
        ArgumentNullException.ThrowIfNull(list);
        ArgumentNullException.ThrowIfNull(indices);

        var result = new List<T>();
        foreach (var index in indices) result.Add(list[index]);
        return result;
    }

    public static (List<int> newNodesFromOld, List<int> oldNodesFromNew) GetNodeMapsFromKillList(
        int maxNodeValue, List<int> KillList)
    {
        if (maxNodeValue < 0) return ([], []);
        var nodeExistenceFlags = new bool[maxNodeValue + 1];
        for (var i = 0; i < nodeExistenceFlags.Length; ++i)
            nodeExistenceFlags[i] = true;

        foreach (var node in KillList)
            if (node >= 0 && node <= maxNodeValue)
                nodeExistenceFlags[node] = false;

        var oldToNewNodeIndexMap = new int[maxNodeValue + 1];
        var newToOldNodeIndexMap = new List<int>();
        var newNodeIndex = 0;
        for (var oldIndex = 0; oldIndex <= maxNodeValue; oldIndex++)
            if (nodeExistenceFlags[oldIndex])
            {
                oldToNewNodeIndexMap[oldIndex] = newNodeIndex++;
                newToOldNodeIndexMap.Add(oldIndex);
            }
            else
            {
                oldToNewNodeIndexMap[oldIndex] = -1;
            }

        return (new List<int>(oldToNewNodeIndexMap), newToOldNodeIndexMap);
    }

    public static void SortUnique<T>(this List<T> list) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(list);
        if (list.Count <= 1) return;

        list.Sort();
        var writeIndex = 1;
        for (var readIndex = 1; readIndex < list.Count; readIndex++)
            if (list[readIndex].CompareTo(list[writeIndex - 1]) != 0)
            {
                if (writeIndex != readIndex)
                    list[writeIndex] = list[readIndex];
                writeIndex++;
            }

        if (writeIndex < list.Count)
            list.RemoveRange(writeIndex, list.Count - writeIndex);
    }

    // BUG FIX: The original implementation of Intersect, Union, Difference, and
    // SymmetricDifference was incorrect. It assumed the input lists were sorted,
    // which is not guaranteed. The corrected versions below now handle unsorted
    // input by creating sorted, unique copies before processing.

    public static List<T> Intersect<T>(List<T> first, List<T> second) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        var sortedFirst = new List<T>(first);
        SortUnique(sortedFirst);
        var sortedSecond = new List<T>(second);
        SortUnique(sortedSecond);

        var result = new List<T>();
        int firstIdx = 0, secondIdx = 0;
        while (firstIdx < sortedFirst.Count && secondIdx < sortedSecond.Count)
        {
            var comparison = sortedFirst[firstIdx].CompareTo(sortedSecond[secondIdx]);
            if (comparison == 0)
            {
                result.Add(sortedFirst[firstIdx]);
                firstIdx++;
                secondIdx++;
            }
            else if (comparison < 0)
            {
                firstIdx++;
            }
            else
            {
                secondIdx++;
            }
        }

        return result;
    }

    public static List<T> Union<T>(List<T> first, List<T> second) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        var sortedFirst = new List<T>(first);
        SortUnique(sortedFirst);
        var sortedSecond = new List<T>(second);
        SortUnique(sortedSecond);

        var result = new List<T>();
        int firstIdx = 0, secondIdx = 0;
        while (firstIdx < sortedFirst.Count && secondIdx < sortedSecond.Count)
        {
            var comparison = sortedFirst[firstIdx].CompareTo(sortedSecond[secondIdx]);
            if (comparison == 0)
            {
                result.Add(sortedFirst[firstIdx]);
                firstIdx++;
                secondIdx++;
            }
            else if (comparison < 0)
            {
                result.Add(sortedFirst[firstIdx]);
                firstIdx++;
            }
            else
            {
                result.Add(sortedSecond[secondIdx]);
                secondIdx++;
            }
        }

        while (firstIdx < sortedFirst.Count) result.Add(sortedFirst[firstIdx++]);
        while (secondIdx < sortedSecond.Count) result.Add(sortedSecond[secondIdx++]);
        return result;
    }

    public static List<T> Difference<T>(List<T> first, List<T> second) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        var sortedFirst = new List<T>(first);
        SortUnique(sortedFirst);
        var sortedSecond = new List<T>(second);
        SortUnique(sortedSecond);

        var result = new List<T>();
        int firstIdx = 0, secondIdx = 0;
        while (firstIdx < sortedFirst.Count && secondIdx < sortedSecond.Count)
        {
            var comparison = sortedFirst[firstIdx].CompareTo(sortedSecond[secondIdx]);
            if (comparison == 0)
            {
                firstIdx++;
                secondIdx++;
            }
            else if (comparison < 0)
            {
                result.Add(sortedFirst[firstIdx]);
                firstIdx++;
            }
            else
            {
                secondIdx++;
            }
        }

        while (firstIdx < sortedFirst.Count) result.Add(sortedFirst[firstIdx++]);
        return result;
    }


    public static List<T> SymmetricDifference<T>(List<T> first, List<T> second) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        var sortedFirst = new List<T>(first);
        SortUnique(sortedFirst);
        var sortedSecond = new List<T>(second);
        SortUnique(sortedSecond);

        var result = new List<T>();
        int firstIdx = 0, secondIdx = 0;

        while (firstIdx < sortedFirst.Count && secondIdx < sortedSecond.Count)
        {
            var comparison = sortedFirst[firstIdx].CompareTo(sortedSecond[secondIdx]);
            if (comparison == 0)
            {
                firstIdx++;
                secondIdx++;
            }
            else if (comparison < 0)
            {
                result.Add(sortedFirst[firstIdx]);
                firstIdx++;
            }
            else
            {
                result.Add(sortedSecond[secondIdx]);
                secondIdx++;
            }
        }

        while (firstIdx < sortedFirst.Count) result.Add(sortedFirst[firstIdx++]);
        while (secondIdx < sortedSecond.Count) result.Add(sortedSecond[secondIdx++]);

        return result;
    }

    public static List<int> GetSortOrder<T>(this List<T> list) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(list);

        var indices = Enumerable.Range(0, list.Count).ToList();
        indices.Sort((i, j) => list[i].CompareTo(list[j]));
        return indices;
    }

    public static List<int> GetSortOrder<T>(this List<List<T>> list) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(list);

        var indices = Enumerable.Range(0, list.Count).ToList();
        var comparer = new ListComparer<T>();
        indices.Sort((i, j) => comparer.Compare(list[i], list[j]));
        return indices;
    }


    public static List<int> GetDuplicatePositions<T>(this List<List<T>> list, List<int> order) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(list);
        ArgumentNullException.ThrowIfNull(order);

        var duplicates = new List<int>();
        if (order.Count < 2) return duplicates;

        var comparer = new ListComparer<T>();

        for (var i = 1; i < order.Count; i++)
            if (comparer.Compare(list[order[i]], list[order[i - 1]]) == 0)
                duplicates.Add(order[i]);

        return duplicates;
    }


    public static List<int> GetDuplicatePositions<T>(this List<T> list, List<int> order) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(list);
        ArgumentNullException.ThrowIfNull(order);

        var duplicates = new List<int>();
        if (order.Count < 2) return duplicates;

        for (var i = 1; i < order.Count; i++)
            if (list[order[i]].CompareTo(list[order[i - 1]]) == 0)
                duplicates.Add(order[i]);

        return duplicates;
    }


    public static int Compare<T>(List<T> first, List<T> second) where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        var minLength = Math.Min(first.Count, second.Count);
        for (var i = 0; i < minLength; i++)
        {
            var comparison = first[i].CompareTo(second[i]);
            if (comparison != 0)
                return comparison;
        }

        return first.Count.CompareTo(second.Count);
    }

    public static int CompareLists<T>(List<List<T>> first, List<List<T>> second)
        where T : IComparable<T>
    {
        ArgumentNullException.ThrowIfNull(first, nameof(first));
        ArgumentNullException.ThrowIfNull(second, nameof(second));

        var minLength = Math.Min(first.Count, second.Count);
        var comparer = new ListComparer<T>();

        for (var i = 0; i < minLength; i++)
        {
            var comparison = comparer.Compare(first[i], second[i]);
            if (comparison != 0)
                return comparison;
        }

        // <-- This was Compareto; it must be CompareTo
        return first.Count.CompareTo(second.Count);
    }
}

public class ListComparer<T> : IComparer<List<T>>
    where T : IComparable<T>
{
    public int Compare(List<T>? x, List<T>? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        var min = Math.Min(x.Count, y.Count);
        for (var i = 0; i < min; i++)
        {
            var c = x[i].CompareTo(y[i]);
            if (c != 0) return c;
        }

        return x.Count.CompareTo(y.Count);
    }
}
