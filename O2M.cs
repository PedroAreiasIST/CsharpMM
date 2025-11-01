using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Serialization;
using static System.Runtime.CompilerServices.MethodImplOptions;

namespace mm2;

public class O2M : IComparable<O2M>, IEquatable<O2M>, ICloneable
{
    [JsonInclude] protected List<List<int>> _adjacencies;
    private int? _maxNodeIndexCache;

    public O2M()
    {
        _adjacencies = [];
        _maxNodeIndexCache = null;
    }

    public O2M(int reservedCapacity)
    {
        _adjacencies = new List<List<int>>(reservedCapacity);
        _maxNodeIndexCache = null;
    }

    public O2M(List<List<int>> adjacenciesList)
    {
        ArgumentNullException.ThrowIfNull(adjacenciesList);
        _adjacencies = adjacenciesList;
        _maxNodeIndexCache = null;
    }

    public int Count => _adjacencies.Count;

    public List<int> this[int rowIndex] => _adjacencies[rowIndex];

    public int this[int rowIndex, int columnIndex]
    {
        get
        {
            var row = _adjacencies[rowIndex];
            return row[columnIndex];
        }
    }

    public virtual object Clone()
    {
        var clonedO2m = new O2M(_adjacencies.Count);
        foreach (var row in _adjacencies) clonedO2m._adjacencies.Add(new List<int>(row));
        return clonedO2m;
    }

    public int CompareTo(O2M? other)
    {
        if (other is null) return 1;
        if (Count != other.Count) return Count.CompareTo(other.Count);

        for (var i = 0; i < Count; i++)
        {
            var comparison = CompareRows(this[i], other[i]);
            if (comparison != 0) return comparison;
        }

        return 0;
    }

    public bool Equals(O2M? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        var a = _adjacencies;
        var b = other._adjacencies;
        if (a.Count != b.Count) return false;

        for (var i = 0; i < a.Count; i++)
        {
            var ar = a[i];
            var br = b[i];
            if (ar.Count != br.Count) return false;
            if (!CollectionsMarshal.AsSpan(ar).SequenceEqual(CollectionsMarshal.AsSpan(br))) return false;
        }

        return true;
    }

    [MethodImpl(AggressiveInlining)]
    private static int CompareRows(List<int> left, List<int> right)
    {
        var comparison = left.Count.CompareTo(right.Count);
        if (comparison != 0) return comparison;
        return CollectionsMarshal.AsSpan(left).SequenceCompareTo(CollectionsMarshal.AsSpan(right));
    }

    public void Reserve(int reservedCapacity)
    {
        if (reservedCapacity > _adjacencies.Capacity)
            _adjacencies.Capacity = reservedCapacity;
    }

    public virtual void ClearElement(int elementIndex)
    {
        if (_adjacencies[elementIndex].Count > 0) _maxNodeIndexCache = null;
        _adjacencies[elementIndex].Clear();
    }

    public virtual void ReplaceElement(int elementIndex, List<int> newNodes)
    {
        ArgumentNullException.ThrowIfNull(newNodes);
        _adjacencies[elementIndex] = newNodes;
        _maxNodeIndexCache = null;
    }

    public virtual int AppendElement(List<int> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        _adjacencies.Add(nodes);
        _maxNodeIndexCache = null;
        return Count - 1;
    }

    public virtual void AppendElements(params List<int>[] nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        foreach (var nodeList in nodes) AppendElement(nodeList);
    }

    public virtual void AppendNodeToElement(int elementIndex, int nodeValue)
    {
        _adjacencies[elementIndex].Add(nodeValue);
        _maxNodeIndexCache = null;
    }

    public virtual bool RemoveNodeFromElement(int elementIndex, int nodeValue)
    {
        var row = _adjacencies[elementIndex];
        var removed = row.Remove(nodeValue);
        if (removed) _maxNodeIndexCache = null;
        return removed;
    }

    public static implicit operator O2M(List<List<int>> nodes)
    {
        return new O2M(nodes);
    }

    public static implicit operator O2M(List<int> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);
        var nodes = new List<List<int>>(elements.Count);
        for (var i = 0; i < elements.Count; i++) nodes.Add([elements[i]]);
        return new O2M(nodes);
    }

    public static implicit operator List<List<int>>(O2M o2mInstance)
    {
        return o2mInstance._adjacencies;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as O2M);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        var count = _adjacencies.Count;
        hashCode.Add(count);

        if (count > 0)
        {
            var firstRow = _adjacencies[0];
            foreach (var item in firstRow) hashCode.Add(item);

            if (count > 1)
            {
                var lastRow = _adjacencies[count - 1];
                foreach (var item in lastRow) hashCode.Add(item);
            }

            if (count > 2)
            {
                var middleRow = _adjacencies[count / 2];
                foreach (var item in middleRow) hashCode.Add(item);
            }
        }

        hashCode.Add(GetMaxNode());
        return hashCode.ToHashCode();
    }

    public static bool operator ==(O2M? left, O2M? right)
    {
        return ReferenceEquals(left, right) || left?.Equals(right) == true;
    }

    public static bool operator !=(O2M? left, O2M? right)
    {
        return !(left == right);
    }

    public static bool operator <(O2M? left, O2M? right)
    {
        return left is null ? right is not null : left.CompareTo(right) < 0;
    }

    public static bool operator >(O2M? left, O2M? right)
    {
        return right < left;
    }

    public static bool operator <=(O2M? left, O2M? right)
    {
        return !(left > right);
    }

    public static bool operator >=(O2M? left, O2M? right)
    {
        return !(left < right);
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        var count = Count;
        for (var i = 0; i < count; i++)
        {
            sb.Append("[").Append(i).Append("] -> ");
            var row = _adjacencies[i];
            sb.AppendLine(row.Count > 0 ? string.Join(", ", row) : "<empty>");
        }

        return sb.ToString();
    }


    [MethodImpl(AggressiveInlining)]
    private static bool ShouldParallelize(int workItems)
    {
        return workItems >= 4096;
        // tweak threshold for your workload
    }


    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public static List<List<int>> GetCliques(O2M elementsToNodes, O2M nodesToElements)
    {
        ArgumentNullException.ThrowIfNull(elementsToNodes);
        ArgumentNullException.ThrowIfNull(nodesToElements);

        var e2nAdj = elementsToNodes._adjacencies;
        var elementCount = e2nAdj.Count;
        if (elementCount == 0) return [];

        // Unique nodes -> sorted -> compact ids
        var unique = new HashSet<int>();
        for (var i = 0; i < elementCount; i++)
        {
            var row = e2nAdj[i];
            for (var j = 0; j < row.Count; j++) unique.Add(row[j]);
        }

        var sorted = new List<int>(unique);
        sorted.Sort();

        var map = new Dictionary<int, int>(sorted.Count);
        for (var i = 0; i < sorted.Count; i++) map[sorted[i]] = i;

        var result = new List<int>[elementCount];

        if (ShouldParallelize(elementCount))
            Parallel.For(0, elementCount, elementId => { BuildCliqueForElement(e2nAdj, elementId, map, result); });
        else
            for (var elementId = 0; elementId < elementCount; elementId++)
                BuildCliqueForElement(e2nAdj, elementId, map, result);

        return new List<List<int>>(result);

        // local worker avoids capture allocs
        static void BuildCliqueForElement(List<List<int>> e2nAdj, int elementId, Dictionary<int, int> map,
            List<int>[] outArr)
        {
            var nodes = e2nAdj[elementId];
            var n = nodes.Count;
            if (n == 0)
            {
                outArr[elementId] = [];
                return;
            }

            var tmp = ArrayPool<int>.Shared.Rent(n);
            for (var i = 0; i < n; i++) tmp[i] = map[nodes[i]];

            var list = new List<int>(new int[n * n]);
            var span = CollectionsMarshal.AsSpan(list);
            var p = 0;
            for (var i = 0; i < n; i++)
            for (var j = 0; j < n; j++)
                span[p++] = tmp[j];

            outArr[elementId] = list;
            ArrayPool<int>.Shared.Return(tmp);
        }
    }

    public bool IsAcyclic()
    {
        var maxNodeValue = GetMaxNode();
        if (maxNodeValue < 0) return true;
        var nodeCount = Math.Max(Count, maxNodeValue + 1);
        var state = new byte[nodeCount]; // 0=unseen,1=onstack,2=done
        var stack = new Stack<int>(Math.Min(nodeCount, 1024));

        for (var start = 0; start < nodeCount; start++)
        {
            if (state[start] != 0) continue;
            stack.Push(start);
            while (stack.Count > 0)
            {
                var u = stack.Peek();
                if (state[u] == 0)
                {
                    state[u] = 1; // enter
                    if (u < Count)
                    {
                        var neigh = _adjacencies[u];
                        var span = CollectionsMarshal.AsSpan(neigh);
                        for (var i = 0; i < span.Length; i++)
                        {
                            var v = span[i];
                            if ((uint)v >= (uint)nodeCount) continue;
                            if (state[v] == 1) return false; // back-edge
                            if (state[v] == 0) stack.Push(v);
                        }
                    }
                }
                else
                {
                    stack.Pop();
                    state[u] = 2; // exit
                }
            }
        }

        return true;
    }

    public bool IsPermutationOf(O2M? other)
    {
        if (other is null) return false;
        if (Count != other.Count) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Count == 0) return true;

        var orderA = GetOrder();
        var orderB = other.GetOrder();
        for (var i = 0; i < Count; i++)
        {
            var a = this[orderA[i]];
            var b = other[orderB[i]];
            if (CompareRows(a, b) != 0) return false;
        }

        return true;
    }

    public List<int> GetTopOrder()
    {
        var maxNodeValue = GetMaxNode();
        var nodeCount = Math.Max(Count, maxNodeValue + 1);
        if (nodeCount == 0) return [];

        var inDeg = new int[nodeCount];
        foreach (var row in _adjacencies)
        {
            var span = CollectionsMarshal.AsSpan(row);
            for (var i = 0; i < span.Length; i++)
            {
                var v = span[i];
                if ((uint)v < (uint)nodeCount) inDeg[v]++;
            }
        }

        var q = new Queue<int>();
        for (var u = 0; u < nodeCount; u++)
            if (inDeg[u] == 0)
                q.Enqueue(u);

        var order = new List<int>(nodeCount);
        while (q.TryDequeue(out var u))
        {
            order.Add(u);
            if (u < Count)
            {
                var span = CollectionsMarshal.AsSpan(_adjacencies[u]);
                for (var i = 0; i < span.Length; i++)
                {
                    var v = span[i];
                    if ((uint)v < (uint)nodeCount && --inDeg[v] == 0) q.Enqueue(v);
                }
            }
        }

        return order;
    }

    public List<int> GetOrder()
    {
        var n = _adjacencies.Count;
        if (n == 0) return [];

        var idx = new int[n];
        for (var i = 0; i < n; i++) idx[i] = i;
        Array.Sort(idx, new RowIndexComparer(_adjacencies));
        return new List<int>(idx);
    }

    public List<int> GetDuplicates()
    {
        var n = Count;
        if (n == 0) return [];
        var sorted = GetOrder();
        var dup = new List<int>();
        for (var k = 0; k < n - 1; k++)
        {
            var r1 = _adjacencies[sorted[k]];
            var r2 = _adjacencies[sorted[k + 1]];
            if (CompareRows(r1, r2) == 0) dup.Add(sorted[k + 1]);
        }

        return dup;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public virtual void CompressElements(List<int> newToOldElementMap)
    {
        ArgumentNullException.ThrowIfNull(newToOldElementMap);
        var oldCount = _adjacencies.Count;
        var newCount = newToOldElementMap.Count;
        var newAdj = new List<List<int>>(newCount);
        var used = new bool[oldCount];

        for (var newIdx = 0; newIdx < newCount; newIdx++)
        {
            var oldIdx = newToOldElementMap[newIdx];
            if ((uint)oldIdx < (uint)oldCount && !used[oldIdx])
            {
                used[oldIdx] = true;
                newAdj.Add(_adjacencies[oldIdx]);
            }
        }

        _adjacencies = newAdj;
        _maxNodeIndexCache = null;
    }

    [MethodImpl(AggressiveInlining)]
    public int GetMaxNode()
    {
        if (_maxNodeIndexCache.HasValue) return _maxNodeIndexCache.Value;
        var max = -1;
        var lists = _adjacencies;
        for (var i = 0; i < lists.Count; i++)
        {
            var span = CollectionsMarshal.AsSpan(lists[i]);
            for (var j = 0; j < span.Length; j++)
                if (span[j] > max)
                    max = span[j];
        }

        _maxNodeIndexCache = max;
        return max;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public virtual void PermuteElements(List<int> oldToNewElementMap)
    {
        ArgumentNullException.ThrowIfNull(oldToNewElementMap);
        var n = Count;
        if (oldToNewElementMap.Count != n) return;

        // Check proper permutation [0..n)
        var seen = new bool[n];
        for (var old = 0; old < n; old++)
        {
            var nw = oldToNewElementMap[old];
            if ((uint)nw >= (uint)n || seen[nw])
            {
                // Fallback: compress semantics, safe for out-of-range / duplicates
                var newIndexToOld = new List<int>(n);
                for (var i = 0; i < n; i++) newIndexToOld.Add(-1);
                for (var o = 0; o < n; o++)
                {
                    var k = oldToNewElementMap[o];
                    if ((uint)k < (uint)n && newIndexToOld[k] == -1) newIndexToOld[k] = o;
                }

                CompressElements(newIndexToOld);
                return;
            }

            seen[nw] = true;
        }

        // Fast path: direct reorder
        var reordered = new List<List<int>>(n);
        reordered.AddRange(new List<int>[n]);
        for (var old = 0; old < n; old++)
        {
            var nw = oldToNewElementMap[old];
            reordered[nw] = _adjacencies[old];
        }

        _adjacencies = reordered;
        _maxNodeIndexCache = null;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public virtual void PermuteNodes(List<int> oldToNewNodeMap)
    {
        ArgumentNullException.ThrowIfNull(oldToNewNodeMap);
        var map = CollectionsMarshal.AsSpan(oldToNewNodeMap);
        var lists = _adjacencies;
        for (var i = 0; i < lists.Count; i++)
        {
            var row = lists[i];
            var span = CollectionsMarshal.AsSpan(row);
            for (var j = 0; j < span.Length; j++)
            {
                var oldIdx = span[j];
                if ((uint)oldIdx < (uint)map.Length) span[j] = map[oldIdx];
            }
        }

        _maxNodeIndexCache = null;
    }

    public virtual void RearrangeAfterRenumbering(List<int> newToOldElementMap, List<int> oldToNewNodeMap)
    {
        ArgumentNullException.ThrowIfNull(newToOldElementMap);
        ArgumentNullException.ThrowIfNull(oldToNewNodeMap);
        CompressElements(newToOldElementMap);
        PermuteNodes(oldToNewNodeMap);
    }

    public (int[] rowPtr, int[] columnIndices) ToCsr()
    {
        var m = _adjacencies.Count;
        var rowPtr = new int[m + 1];
        var nnz = 0;
        for (var i = 0; i < m; i++) nnz += _adjacencies[i].Count;
        var col = new int[nnz];

        var offset = 0;
        rowPtr[0] = 0;
        for (var i = 0; i < m; i++)
        {
            var src = CollectionsMarshal.AsSpan(_adjacencies[i]);
            src.CopyTo(col.AsSpan(offset));
            offset += src.Length;
            rowPtr[i + 1] = offset;
        }

        return (rowPtr, col);
    }

    public static O2M FromCsr(int[] rowPointers, int[] columnIndices)
    {
        ArgumentNullException.ThrowIfNull(rowPointers);
        ArgumentNullException.ThrowIfNull(columnIndices);
        if (rowPointers.Length == 0) return new O2M();

        var m = rowPointers.Length - 1;
        var o = new O2M(m);
        for (var i = 0; i < m; i++)
        {
            var start = rowPointers[i];
            var end = rowPointers[i + 1];
            var len = end - start;
            var row = new List<int>(new int[len]);
            var span = CollectionsMarshal.AsSpan(row);
            columnIndices.AsSpan(start, len).CopyTo(span);
            o._adjacencies.Add(row);
        }

        return o;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public static List<List<int>> GetNodePositions(O2M nodesFromElement, O2M elementsFromNode)
    {
        ArgumentNullException.ThrowIfNull(nodesFromElement);
        ArgumentNullException.ThrowIfNull(elementsFromNode);
        var nodeCount = elementsFromNode.Count;
        var nodePos = new List<List<int>>(nodeCount);
        for (var i = 0; i < nodeCount; i++) nodePos.Add([]);

        for (var e = 0; e < nodesFromElement.Count; e++)
        {
            var row = nodesFromElement._adjacencies[e];
            var span = CollectionsMarshal.AsSpan(row);
            for (var loc = 0; loc < span.Length; loc++)
            {
                var node = span[loc];
                if ((uint)node < (uint)nodeCount) nodePos[node].Add(loc);
            }
        }

        return nodePos;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public static List<List<int>> GetElementPositions(O2M elementsToNodes, O2M nodesToElements)
    {
        ArgumentNullException.ThrowIfNull(elementsToNodes);
        ArgumentNullException.ThrowIfNull(nodesToElements);
        var elemCount = elementsToNodes.Count;

        var elemPos = new List<List<int>>(elemCount);
        for (var e = 0; e < elemCount; e++) elemPos.Add(new List<int>(new int[elementsToNodes._adjacencies[e].Count]));

        // Build node -> (element -> position) maps once
        var maps = new Dictionary<int, int>[nodesToElements.Count];
        for (var n = 0; n < nodesToElements.Count; n++)
        {
            var elems = nodesToElements._adjacencies[n];
            var d = new Dictionary<int, int>(elems.Count);
            for (var i = 0; i < elems.Count; i++) d[elems[i]] = i;
            maps[n] = d;
        }

        if (ShouldParallelize(elemCount))
            Parallel.For(0, elemCount, e =>
            {
                var nodes = elementsToNodes._adjacencies[e];
                for (var j = 0; j < nodes.Count; j++)
                {
                    var n = nodes[j];
                    if ((uint)n < (uint)maps.Length && maps[n].TryGetValue(e, out var pos))
                        elemPos[e][j] = pos;
                }
            });
        else
            for (var e = 0; e < elemCount; e++)
            {
                var nodes = elementsToNodes._adjacencies[e];
                for (var j = 0; j < nodes.Count; j++)
                {
                    var n = nodes[j];
                    if ((uint)n < (uint)maps.Length && maps[n].TryGetValue(e, out var pos))
                        elemPos[e][j] = pos;
                }
            }

        return elemPos;
    }

    public bool[,] ToBooleanMatrix()
    {
        var maxNodeValue = GetMaxNode();
        var rowCount = Count;
        var colCount = maxNodeValue + 1;
        if (colCount <= 0) return new bool[rowCount, 0];
        var m = new bool[rowCount, colCount];
        for (var i = 0; i < rowCount; i++)
        {
            var span = CollectionsMarshal.AsSpan(_adjacencies[i]);
            for (var k = 0; k < span.Length; k++)
            {
                var v = span[k];
                if ((uint)v < (uint)colCount) m[i, v] = true;
            }
        }

        return m;
    }

    public static O2M FromBooleanMatrix(bool[,] matrix)
    {
        ArgumentNullException.ThrowIfNull(matrix);
        var r = matrix.GetLength(0);
        var c = matrix.GetLength(1);
        var o = new O2M(r);
        for (var i = 0; i < r; i++)
        {
            var nnz = 0;
            for (var j = 0; j < c; j++)
                if (matrix[i, j])
                    nnz++;
            var row = new List<int>(new int[nnz]);
            var span = CollectionsMarshal.AsSpan(row);
            var p = 0;
            for (var j = 0; j < c; j++)
                if (matrix[i, j])
                    span[p++] = j;
            o._adjacencies.Add(row);
        }

        return o;
    }

    public static O2M GetRandomO2M(int elementCount, int nodeCount, double density, int? seed = null)
    {
        var rnd = seed.HasValue ? new Random(seed.Value) : Random.Shared;
        var o = new O2M(elementCount);
        var expected = (int)Math.Round(nodeCount * Math.Clamp(density, 0.0, 1.0));
        for (var i = 0; i < elementCount; i++)
        {
            var row = new List<int>(Math.Max(expected, 4));
            for (var j = 0; j < nodeCount; j++)
                if (rnd.NextDouble() < density)
                    row.Add(j);
            o.AppendElement(row);
        }

        return o;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    public O2M Transpose()
    {
        var sourceCount = Count;
        if (sourceCount == 0)
            return new O2M();

        var maxNode = GetMaxNode();
        if (maxNode < 0) return new O2M();

        var targetCount = Math.Max(sourceCount, maxNode + 1);
        var result = new O2M(targetCount);
        result._adjacencies.Clear();

        var chunkCount = Math.Min(Environment.ProcessorCount, sourceCount);
        var perChunkNodeCounts = new int[chunkCount][];
        var chunkSize = sourceCount / chunkCount;
        var remainder = sourceCount % chunkCount;

        for (var c = 0; c < chunkCount; c++)
        {
            // No need for complex AVX zeroing for the subsequent logic,
            // Array.Clear is sufficient and simple.
            var localArray = new int[targetCount];
            perChunkNodeCounts[c] = localArray;
        }

        // --- PASS 1: Simplified Parallel Counting ---
        Parallel.For(0, chunkCount, c =>
        {
            var start = c * chunkSize + Math.Min(c, remainder);
            var end = start + chunkSize + (c < remainder ? 1 : 0);
            var localCounts = perChunkNodeCounts[c];

            for (var i = start; i < end; i++)
            {
                var adj = _adjacencies[i];
                foreach (var targetNode in adj) localCounts[targetNode]++;
            }
        });

        var adjacencyLists = new List<int>[targetCount];

        // --- PASS 2: Allocation and Offset Calculation ---
        Parallel.For(0, targetCount, t =>
        {
            var totalOccurrences = 0;
            for (var c = 0; c < chunkCount; c++)
                totalOccurrences += perChunkNodeCounts[c][t];

            if (totalOccurrences > 0)
            {
                var list = new List<int>(totalOccurrences);
                for (var k = 0; k < totalOccurrences; k++)
                    list.Add(0);
                adjacencyLists[t] = list;
            }

            var offset = 0;
            for (var c = 0; c < chunkCount; c++)
            {
                var count = perChunkNodeCounts[c][t];
                perChunkNodeCounts[c][t] = offset;
                offset += count;
            }
        });

        // --- PASS 3: Simplified Parallel Filling ---
        Parallel.For(0, chunkCount, c =>
        {
            var start = c * chunkSize + Math.Min(c, remainder);
            var end = start + chunkSize + (c < remainder ? 1 : 0);
            var offsets = perChunkNodeCounts[c];

            for (var i = start; i < end; i++)
            {
                var adj = _adjacencies[i];
                foreach (var target in adj)
                {
                    // This logic remains brilliant. Find the pre-calculated slot and fill it.
                    var writeIndex = offsets[target]++;
                    adjacencyLists[target][writeIndex] = i;
                }
            }
        });

        // Fill the result adjacencies, handling nulls for nodes with no incoming edges
        for (var i = 0; i < targetCount; i++) result._adjacencies.Add(adjacencyLists[i] ?? new List<int>(0));

        return result;
    }

    public string ToEpsString()
    {
        const int Margin = 40;
        const int ElemSpacing = 20;
        const int NodeSpacing = 20;
        const int ElemRadius = 4;
        const int NodeRadius = 4;
        const double LineWidth = 0.5;
        const string ElementColor = "0 0 0";
        const string NodeColor = "0 0 0";
        const string LineColor = "0.5 0.5 0.5";
        const string TextColor = "0 0 0";
        const int FontSize = 12;
        const bool DrawElementLabels = true;
        const bool DrawNodeLabels = true;

        var elementCount = Count;
        var maxNodeValue = GetMaxNode();
        var elementsAreaWidth = elementCount > 0 ? 2 * ElemRadius : 0;
        var nodesAreaWidth = maxNodeValue >= 0 ? NodeSpacing + maxNodeValue * NodeSpacing + 2 * NodeRadius : 0;
        var contentWidth = elementsAreaWidth + (nodesAreaWidth > 0 ? nodesAreaWidth : 0);
        var elementsAreaHeight = elementCount > 0 ? 2 * ElemRadius + (elementCount - 1) * ElemSpacing : 0;
        var finalWidth = 2 * Margin + contentWidth;
        var finalHeight = 2 * Margin + elementsAreaHeight;

        var sb = new StringBuilder();
        sb.AppendLine("%!PS-Adobe-3.0 EPSF-3.0");
        sb.AppendLine($"%%BoundingBox: 0 0 {finalWidth} {finalHeight}");
        sb.AppendLine($"%%Title: {EscapePS("Sparse relation")}");
        sb.AppendLine("%%Creator: O2M.ToEpsString");
        sb.AppendLine("%%EndComments\n");
        sb.AppendLine($"/Times-Roman findfont {FontSize} scalefont setfont\n");
        sb.AppendLine($"{ElementColor} setrgbcolor");

        for (var i = 0; i < elementCount; i++)
        {
            double x = Margin + ElemRadius;
            double y = finalHeight - Margin - ElemRadius - i * ElemSpacing;
            sb.AppendLine($"{x} {y} {ElemRadius} 0 360 arc fill");
            if (DrawElementLabels)
                sb.AppendLine(
                    $"{TextColor} setrgbcolor {x + ElemRadius + FontSize / 3.0} {y - FontSize / 3.0} moveto ({i}) show");
        }

        if (maxNodeValue >= 0)
        {
            sb.AppendLine($"{NodeColor} setrgbcolor");
            for (var j = 0; j <= maxNodeValue; j++)
            {
                double x = Margin + elementsAreaWidth + NodeSpacing + j * NodeSpacing;
                double y = Margin + NodeRadius;
                sb.AppendLine($"{x} {y} {NodeRadius} 0 360 arc fill");
                if (DrawNodeLabels)
                    sb.AppendLine(
                        $"{TextColor} setrgbcolor {x} {y + NodeRadius + FontSize * 0.5} moveto ({j}) dup stringwidth pop 2 div neg 0 rmoveto show");
            }
        }

        if (elementCount > 0 && maxNodeValue >= 0)
        {
            sb.AppendLine($"{LineWidth} setlinewidth");
            sb.AppendLine($"{LineColor} setrgbcolor");
            for (var i = 0; i < elementCount; i++)
            {
                double elemX = Margin + ElemRadius;
                double elemY = finalHeight - Margin - ElemRadius - i * ElemSpacing;
                var startX = elemX + ElemRadius;
                var span = CollectionsMarshal.AsSpan(_adjacencies[i]);
                for (var k = 0; k < span.Length; k++)
                {
                    var node = span[k];
                    if (node > maxNodeValue) continue;
                    double nodeX = Margin + elementsAreaWidth + NodeSpacing + node * NodeSpacing;
                    double nodeY = Margin + NodeRadius;
                    sb.AppendLine($"{startX} {elemY} moveto {nodeX} {nodeY} lineto stroke");
                }
            }
        }

        sb.AppendLine("\nshowpage\n%%EOF");
        return sb.ToString();
    }

    private static string EscapePS(string text)
    {
        return string.IsNullOrEmpty(text)
            ? string.Empty
            : text.Replace("\\", "\\\\").Replace("(", "\\(").Replace(")", "\\)");
    }

    public static O2M operator *(O2M left, O2M right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var leftMax = left.GetMaxNode();
        var product = leftMax < right.Count
            ? PerformSymbolicMultiplicationUnchecked(left._adjacencies, right._adjacencies)
            : PerformSymbolicMultiplicationChecked(left._adjacencies, right._adjacencies);
        return new O2M(product);
    }

    public static O2M operator |(O2M left, O2M right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var elementCount = Math.Max(left.Count, right.Count);
        var result = new O2M(elementCount);

        var domain = Math.Max(left.GetMaxNode(), right.GetMaxNode()) + 1;
        // Small-domain fast path: bitset membership filter, stable left->right order
        if (domain > 0 && domain <= 4096)
        {
            var words = (domain + 31) >> 5;
            Span<uint> bits = stackalloc uint[words];

            for (var i = 0; i < elementCount; i++)
            {
                bits.Clear();
                var row = new List<int>();

                if (i < left.Count)
                {
                    var s = CollectionsMarshal.AsSpan(left._adjacencies[i]);
                    for (var k = 0; k < s.Length; k++)
                    {
                        var v = s[k];
                        if ((uint)v < (uint)domain)
                        {
                            var mask = 1u << (v & 31);
                            var w = v >> 5;
                            if ((bits[w] & mask) == 0)
                            {
                                bits[w] |= mask;
                                row.Add(v);
                            }
                        }
                    }
                }

                if (i < right.Count)
                {
                    var s = CollectionsMarshal.AsSpan(right._adjacencies[i]);
                    for (var k = 0; k < s.Length; k++)
                    {
                        var v = s[k];
                        if ((uint)v < (uint)domain)
                        {
                            var mask = 1u << (v & 31);
                            var w = v >> 5;
                            if ((bits[w] & mask) == 0)
                            {
                                bits[w] |= mask;
                                row.Add(v);
                            }
                        }
                    }
                }

                result._adjacencies.Add(row);
            }

            return result;
        }

        // Fallback: HashSet with stable left-then-right order
        var unionSet = new HashSet<int>();
        for (var i = 0; i < elementCount; i++)
        {
            unionSet.Clear();
            var row = new List<int>();

            if (i < left.Count)
            {
                var spanL = CollectionsMarshal.AsSpan(left._adjacencies[i]);
                for (var k = 0; k < spanL.Length; k++)
                {
                    var v = spanL[k];
                    if (unionSet.Add(v)) row.Add(v);
                }
            }

            if (i < right.Count)
            {
                var spanR = CollectionsMarshal.AsSpan(right._adjacencies[i]);
                for (var k = 0; k < spanR.Length; k++)
                {
                    var v = spanR[k];
                    if (unionSet.Add(v)) row.Add(v);
                }
            }

            result._adjacencies.Add(row);
        }

        return result;
    }

    public static O2M operator +(O2M left, O2M right)
    {
        return left | right;
    }

    public static O2M operator &(O2M left, O2M right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var elementCount = Math.Min(left.Count, right.Count);
        var result = new O2M(elementCount);

        var domain = Math.Max(left.GetMaxNode(), right.GetMaxNode()) + 1;
        if (domain > 0 && domain <= 4096)
        {
            var words = (domain + 31) >> 5;
            Span<uint> bits = stackalloc uint[words];

            for (var i = 0; i < elementCount; i++)
            {
                bits.Clear();
                // mark right row
                var rr = right._adjacencies[i];
                var rspan = CollectionsMarshal.AsSpan(rr);
                for (var k = 0; k < rspan.Length; k++)
                {
                    var v = rspan[k];
                    if ((uint)v < (uint)domain) bits[v >> 5] |= 1u << (v & 31);
                }

                // probe left in its order
                var lr = left._adjacencies[i];
                var lspan = CollectionsMarshal.AsSpan(lr);
                var res = new List<int>(Math.Min(lr.Count, rr.Count));
                for (var k = 0; k < lspan.Length; k++)
                {
                    var v = lspan[k];
                    if ((uint)v < (uint)domain && ((bits[v >> 5] >> (v & 31)) & 1u) != 0) res.Add(v);
                }

                result._adjacencies.Add(res);
            }

            return result;
        }

        // Fallback: HashSet, iterate larger to keep previous semantics
        var set = new HashSet<int>();
        for (var i = 0; i < elementCount; i++)
        {
            var L = left._adjacencies[i];
            var R = right._adjacencies[i];
            var small = L.Count <= R.Count ? L : R;
            var big = L.Count <= R.Count ? R : L;

            set.Clear();
            var sspan = CollectionsMarshal.AsSpan(small);
            for (var k = 0; k < sspan.Length; k++) set.Add(sspan[k]);

            var res = new List<int>(set.Count);
            var bspan = CollectionsMarshal.AsSpan(big);
            for (var k = 0; k < bspan.Length; k++)
                if (set.Contains(bspan[k]))
                    res.Add(bspan[k]);
            result._adjacencies.Add(res);
        }

        return result;
    }

    public static O2M operator ^(O2M left, O2M right)
    {
        return (left | right) - (left & right);
    }

    public static O2M operator -(O2M left, O2M right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var elementCount = left.Count;
        var result = new O2M(elementCount);

        var domain = Math.Max(left.GetMaxNode(), right.GetMaxNode()) + 1;
        if (domain > 0 && domain <= 4096)
        {
            var words = (domain + 31) >> 5;
            Span<uint> bits = stackalloc uint[words];

            for (var i = 0; i < elementCount; i++)
            {
                bits.Clear();
                if (i < right.Count)
                {
                    var r = CollectionsMarshal.AsSpan(right._adjacencies[i]);
                    for (var k = 0; k < r.Length; k++)
                    {
                        var v = r[k];
                        if ((uint)v < (uint)domain) bits[v >> 5] |= 1u << (v & 31);
                    }
                }

                var lrow = left._adjacencies[i];
                var lspan = CollectionsMarshal.AsSpan(lrow);
                var res = new List<int>(lrow.Count);
                for (var k = 0; k < lspan.Length; k++)
                {
                    var v = lspan[k];
                    if (!((uint)v < (uint)domain && ((bits[v >> 5] >> (v & 31)) & 1u) != 0))
                        res.Add(v);
                }

                result._adjacencies.Add(res);
            }

            return result;
        }

        // Fallback: HashSet
        var set = new HashSet<int>();
        for (var i = 0; i < elementCount; i++)
        {
            set.Clear();
            if (i < right.Count)
            {
                var spanR = CollectionsMarshal.AsSpan(right._adjacencies[i]);
                for (var k = 0; k < spanR.Length; k++) set.Add(spanR[k]);
            }

            var leftRow = left._adjacencies[i];
            var spanL = CollectionsMarshal.AsSpan(leftRow);
            var res = new List<int>(leftRow.Count);
            for (var k = 0; k < spanL.Length; k++)
                if (!set.Contains(spanL[k]))
                    res.Add(spanL[k]);
            result._adjacencies.Add(res);
        }

        return result;
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    private static List<List<int>> PerformSymbolicMultiplicationUnchecked(List<List<int>> aRows, List<List<int>> bRows)
    {
        var rowCount = aRows.Count;
        var productRows = new List<int>[rowCount];
        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        Parallel.For(0, rowCount, parallelOptions,
            () => (new HashSet<int>(256), new List<int>(256)),
            (i, _, state) =>
            {
                var (localSet, resultList) = state;
                localSet.Clear();
                resultList.Clear();

                var aRow = aRows[i];
                var aSpan = CollectionsMarshal.AsSpan(aRow);
                for (var j = 0; j < aSpan.Length; j++)
                {
                    var bSpan = CollectionsMarshal.AsSpan(bRows[aSpan[j]]);
                    for (var k = 0; k < bSpan.Length; k++)
                        localSet.Add(bSpan[k]);
                }

                resultList.AddRange(localSet);

                productRows[i] = resultList;
                return (localSet, resultList);
            },
            _ => { });

        return new List<List<int>>(productRows);
    }

    [MethodImpl(AggressiveOptimization | AggressiveInlining)]
    [SkipLocalsInit]
    private static List<List<int>> PerformSymbolicMultiplicationChecked(List<List<int>> aRows, List<List<int>> bRows)
    {
        var rowCount = aRows.Count;
        var productRows = new List<int>[rowCount];

        if (ShouldParallelize(rowCount))
        {
            Parallel.For(0, rowCount,
                () => new HashSet<int>(64),
                (i, _, localSet) =>
                {
                    localSet.Clear();
                    var aRow = aRows[i];
                    for (var j = 0; j < aRow.Count; j++)
                    {
                        var mid = aRow[j];
                        if ((uint)mid >= (uint)bRows.Count) continue;
                        var bspan = CollectionsMarshal.AsSpan(bRows[mid]);
                        for (var k = 0; k < bspan.Length; k++) localSet.Add(bspan[k]);
                    }

                    productRows[i] = new List<int>(localSet);
                    return localSet;
                },
                _ => { });
        }
        else
        {
            var localSet = new HashSet<int>(64);
            for (var i = 0; i < rowCount; i++)
            {
                localSet.Clear();
                var aRow = aRows[i];
                for (var j = 0; j < aRow.Count; j++)
                {
                    var mid = aRow[j];
                    if ((uint)mid >= (uint)bRows.Count) continue;
                    var bspan = CollectionsMarshal.AsSpan(bRows[mid]);
                    for (var k = 0; k < bspan.Length; k++) localSet.Add(bspan[k]);
                }

                productRows[i] = new List<int>(localSet);
            }
        }

        return new List<List<int>>(productRows);
    }

    public bool IsValid()
    {
        var set = new HashSet<int>();
        foreach (var row in _adjacencies)
        {
            set.Clear();
            var span = CollectionsMarshal.AsSpan(row);
            for (var i = 0; i < span.Length; i++)
            {
                var v = span[i];
                if (v < 0) return false;
                if (!set.Add(v)) return false;
            }
        }

        return true;
    }

    public void ShrinkToFit()
    {
        _adjacencies.TrimExcess();
        for (var i = 0; i < _adjacencies.Count; i++) _adjacencies[i].TrimExcess();
    }

    public virtual void ClearAll()
    {
        _adjacencies.Clear();
        _maxNodeIndexCache = null;
    }

    private sealed class RowIndexComparer : IComparer<int>
    {
        private readonly List<List<int>> _adj;

        public RowIndexComparer(List<List<int>> adj)
        {
            _adj = adj;
        }

        [MethodImpl(AggressiveInlining)]
        public int Compare(int i, int j)
        {
            return CompareRows(_adj[i], _adj[j]);
        }
    }
}
