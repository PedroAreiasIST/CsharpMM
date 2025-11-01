namespace mm2;

public class M2M : O2M, IComparable<M2M>, IEquatable<M2M>
{
    private readonly object _syncLock = new(); // FIX: Added thread safety

    public M2M(int ReservedSize) : base(ReservedSize)
    {
        InvalidateSync();
    }

    public M2M(List<List<int>> Adjacencies) : base(Adjacencies)
    {
        InvalidateSync();
    }

    public M2M()
    {
        InvalidateSync();
    }

    public IReadOnlyList<IReadOnlyList<int>> Elemeloc { get; private set; } = Array.Empty<IReadOnlyList<int>>();
    public IReadOnlyList<IReadOnlyList<int>> Nodeloc { get; private set; } = Array.Empty<IReadOnlyList<int>>();
    private bool IsInSync { get; set; }
    private bool _batchMode { get; set; } // FIX: Added batch mode flag
    public O2M ElementsFromNode { get; private set; } = new();

    public int CompareTo(M2M? other)
    {
        if (other is null) return 1;
        if (Count != other.Count) return Count.CompareTo(other.Count);
        for (var i = 0; i < Count; i++)
        {
            var comparison = Utils.Compare(this[i], other[i]);
            if (comparison != 0) return comparison;
        }

        return 0;
    }

    public bool Equals(M2M? other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is null || Count != other.Count) return false;
        for (var i = 0; i < Count; i++)
            if (Utils.Compare(this[i], other[i]) != 0)
                return false;
        return true;
    }

    private void InvalidateSync()
    {
        lock (_syncLock)
        {
            IsInSync = false;
        }
    }

    // FIX: Added batch update methods for performance
    public void BeginBatchUpdate()
    {
        lock (_syncLock)
        {
            _batchMode = true;
        }
    }

    public void EndBatchUpdate()
    {
        lock (_syncLock)
        {
            _batchMode = false;
            if (!IsInSync)
            {
                SynchronizeInternal();
            }
        }
    }

    // FIX: Improved Clone to properly copy cache state
    public override object Clone()
    {
        lock (_syncLock)
        {
            // Create a new M2M instance from the deeply copied adjacencies
            var adjacenciesCopy = new List<List<int>>(Count);
            foreach (var row in _adjacencies) 
                adjacenciesCopy.Add(new List<int>(row));
            
            var cloned = new M2M(adjacenciesCopy);
            
            // FIX: Copy the cache if needed
            if (IsInSync)
            {
                cloned.ElementsFromNode = (O2M)ElementsFromNode.Clone();
                cloned.Elemeloc = Elemeloc;
                cloned.Nodeloc = Nodeloc;
                cloned.IsInSync = true;
            }
            
            return cloned;
        }
    }

    public override int AppendElement(List<int> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        lock (_syncLock)
        {
            InvalidateSync();
            return base.AppendElement(nodes);
        }
    }

    public override void AppendElements(params List<int>[] nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        if (nodes.Length == 0) return;
        lock (_syncLock)
        {
            InvalidateSync();
            base.AppendElements(nodes);
        }
    }

    public override void AppendNodeToElement(int elementIndex, int nodeValue)
    {
        lock (_syncLock)
        {
            InvalidateSync();
            base.AppendNodeToElement(elementIndex, nodeValue);
        }
    }

    public override bool RemoveNodeFromElement(int elementIndex, int nodeValue)
    {
        lock (_syncLock)
        {
            var removed = base.RemoveNodeFromElement(elementIndex, nodeValue);
            if (removed)
                InvalidateSync();
            return removed;
        }
    }

    public override void ClearElement(int element)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(element);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(element, Count);
        lock (_syncLock)
        {
            base.ClearElement(element);
            InvalidateSync();
        }
    }

    public override void ReplaceElement(int element, List<int> newnodes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(element);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(element, Count);
        lock (_syncLock)
        {
            base.ReplaceElement(element, newnodes);
            InvalidateSync();
        }
    }

    private void SynchronizeInternal()
    {
        if (!IsInSync)
        {
            ElementsFromNode = Transpose();
            var elemPositions = GetElementPositions(this, ElementsFromNode);
            var nodePositions = GetNodePositions(this, ElementsFromNode);
            var elemLocList = new List<IReadOnlyList<int>>(elemPositions.Count);
            for (var i = 0; i < elemPositions.Count; i++) 
                elemLocList.Add(elemPositions[i].AsReadOnly());
            Elemeloc = elemLocList.AsReadOnly();
            var nodeLocList = new List<IReadOnlyList<int>>(nodePositions.Count);
            for (var i = 0; i < nodePositions.Count; i++) 
                nodeLocList.Add(nodePositions[i].AsReadOnly());
            Nodeloc = nodeLocList.AsReadOnly();
            IsInSync = true;
        }
    }

    public void Synchronize()
    {
        lock (_syncLock)
        {
            if (_batchMode) return; // Don't sync during batch mode
            SynchronizeInternal();
        }
    }

    public List<int> GetElementsWithNodes(List<int> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        if (nodes.Count == 0) return [];
        
        lock (_syncLock)
        {
            SynchronizeInternal();
            for (var i = 0; i < nodes.Count; i++)
                if (nodes[i] < 0 || nodes[i] >= ElementsFromNode.Count)
                    return [];
            var elems = ElementsFromNode[nodes[0]];
            if (elems.Count == 0) return [];
            var result = new List<int>(elems);
            var nodeCount = nodes.Count;
            for (var node = 1; node < nodeCount && result.Count > 0; node++)
            {
                var nodeElems = ElementsFromNode[nodes[node]];
                if (nodeElems.Count == 0) return [];
                result = Utils.Intersect(result, nodeElems);
            }

            return result;
        }
    }

    public List<int> GetElementsFromNodes(List<int> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        lock (_syncLock)
        {
            var nodeCount = nodes.Count;
            var elements = GetElementsWithNodes(nodes);
            var result = new List<int>();
            foreach (var e in elements)
                if (e < Count && _adjacencies[e].Count == nodeCount)
                    result.Add(e);
            return result;
        }
    }

    public List<int> GetElementNeighbours(int element)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(element);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(element, Count);
        
        lock (_syncLock)
        {
            SynchronizeInternal();
            var neighbours = new HashSet<int>(_adjacencies[element].Count * 4);
            foreach (var node in this[element])
            foreach (var elem in ElementsFromNode[node])
                if (elem != element)
                    neighbours.Add(elem);
            var result = new List<int>(neighbours);
            result.Sort();
            return result;
        }
    }

    public List<int> GetNodeNeighbours(int node)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(node);
        
        lock (_syncLock)
        {
            SynchronizeInternal();
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(node, ElementsFromNode.Count);
            var neighbours = new HashSet<int>();
            foreach (var elem in ElementsFromNode[node])
            foreach (var n in this[elem])
                if (n != node)
                    neighbours.Add(n);
            var result = new List<int>(neighbours);
            result.Sort();
            return result;
        }
    }

    public override void CompressElements(List<int> oldElementFromNew)
    {
        ArgumentNullException.ThrowIfNull(oldElementFromNew);
        if (oldElementFromNew.Count == 0) return;
        lock (_syncLock)
        {
            InvalidateSync();
            base.CompressElements(oldElementFromNew);
        }
    }

    public override void PermuteElements(List<int> newElementFromOld)
    {
        ArgumentNullException.ThrowIfNull(newElementFromOld);
        if (newElementFromOld.Count == 0) return;
        lock (_syncLock)
        {
            InvalidateSync();
            base.PermuteElements(newElementFromOld);
        }
    }

    public override void PermuteNodes(List<int> newNodesFromOld)
    {
        ArgumentNullException.ThrowIfNull(newNodesFromOld);
        if (newNodesFromOld.Count == 0) return;
        lock (_syncLock)
        {
            InvalidateSync();
            base.PermuteNodes(newNodesFromOld);
        }
    }

    public override void RearrangeAfterRenumbering(List<int> newToOldElementMap, List<int> oldToNewNodeMap)
    {
        lock (_syncLock)
        {
            InvalidateSync();
            base.RearrangeAfterRenumbering(newToOldElementMap, oldToNewNodeMap);
        }
    }

    public override void ClearAll()
    {
        lock (_syncLock)
        {
            base.ClearAll();
            InvalidateSync();
            ElementsFromNode.ClearAll();
            Elemeloc = Array.Empty<IReadOnlyList<int>>();
            Nodeloc = Array.Empty<IReadOnlyList<int>>();
        }
    }

    public O2M GetElementsToElements()
    {
        lock (_syncLock)
        {
            SynchronizeInternal();
            return this * ElementsFromNode;
        }
    }

    public O2M GetNodesToNodes()
    {
        lock (_syncLock)
        {
            SynchronizeInternal();
            return ElementsFromNode * this;
        }
    }

    public List<List<int>> GetCliques()
    {
        lock (_syncLock)
        {
            SynchronizeInternal();
            return GetCliques(this, ElementsFromNode);
        }
    }
}
