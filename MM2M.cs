namespace mm2;

public class MM2M
{
    // FIX: Removed inline initialization - done only in constructor
    public readonly Dictionary<int, HashSet<int>> ListOfMarked;
    private readonly M2M[,] mat;
    private readonly object syncLock = new();

    public MM2M(int numberOfTypes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(numberOfTypes);
        mat = new M2M[numberOfTypes, numberOfTypes];
        for (var i = 0; i < numberOfTypes; i++)
        for (var j = 0; j < numberOfTypes; j++)
            mat[i, j] = new M2M();
        NumberOfTypes = numberOfTypes;
        // FIX: Initialize only once
        ListOfMarked = new Dictionary<int, HashSet<int>>();
        InitializeMarkedCollections();
    }

    public int NumberOfTypes { get; init; }

    public M2M this[int elementType, int nodeType]
    {
        get
        {
            ValidateTypeIndex(elementType);
            ValidateTypeIndex(nodeType);
            return mat[elementType, nodeType];
        }
        set
        {
            ValidateTypeIndex(elementType);
            ValidateTypeIndex(nodeType);
            mat[elementType, nodeType] = value ?? throw new ArgumentNullException(nameof(value));
        }
    }

    public List<(int ElemType, int Elem)> GetAllElements(int nodeType, int node)
    {
        ValidateTypeIndex(nodeType);
        ArgumentOutOfRangeException.ThrowIfNegative(node);
        lock (syncLock)
        {
            var resultSet = new SortedSet<(int ElemType, int Elem)>();
            for (var et = 0; et < NumberOfTypes; et++)
            {
                if (et == nodeType) continue;
                if (node < mat[et, nodeType].ElementsFromNode.Count)
                {
                    var list = mat[et, nodeType].ElementsFromNode[node];
                    resultSet.UnionWith(list.Select(elem => (et, elem)));
                }
            }

            var res = resultSet.ToList();
            return res;
        }
    }

    public int GetNumberOfNodes(int elementType, int element, int nodeType)
    {
        ValidateTypeIndex(elementType);
        ValidateTypeIndex(nodeType);
        if (element < 0)
            return 0;
        lock (syncLock)
        {
            return element >= mat[elementType, nodeType].Count ? 0 : mat[elementType, nodeType][element].Count;
        }
    }

    public int GetNumberOfElements(int nodeType, int node, int elementType)
    {
        ValidateTypeIndex(nodeType);
        ValidateTypeIndex(elementType);
        if (node < 0)
            return 0;
        lock (syncLock)
        {
            return node >= mat[elementType, nodeType].ElementsFromNode.Count
                ? 0
                : mat[elementType, nodeType].ElementsFromNode[node].Count;
        }
    }

    public int GetNumberOfElements(int elementType)
    {
        ValidateTypeIndex(elementType);
        lock (syncLock)
        {
            return mat[elementType, elementType].Count;
        }
    }

    public int GetNumberOfActiveElements(int elementType)
    {
        ValidateTypeIndex(elementType);
        lock (syncLock)
        {
            var targetMatrix = mat[elementType, elementType];
            var count = 0;
            for (var i = 0; i < targetMatrix.Count; i++)
                if (ListOfMarked[elementType].Contains(targetMatrix[i, 0]))
                    count++;
            return count;
        }
    }

    public void MarkToErase(int nodeType, int node)
    {
        ValidateTypeIndex(nodeType);
        ArgumentOutOfRangeException.ThrowIfNegative(node);
        if (ListOfMarked[nodeType].Contains(node)) return;

        MarkNodeForErasure(nodeType, node);
        var dependentElements = DepthFirstSearchFromANode(nodeType, node);
        MarkDependentElementsForErasure(dependentElements);
    }

    public List<(int Type, int Node)> GetAllElements(int nodeType)
    {
        ValidateTypeIndex(nodeType);
        lock (syncLock)
        {
            var result = new List<(int Type, int Node)>();
            for (var n = 0; n < mat[nodeType, nodeType].Count; n++)
            {
                var elements = GetAllElements(nodeType, n);
                foreach (var element in elements)
                    if (!result.Contains(element))
                        result.Add(element);
            }

            result.Sort((x, y) => x.CompareTo(y));
            return result;
        }
    }

    public List<(int Type, int Node)> GetAllNodes(int elementType, int elementNumber)
    {
        ValidateTypeIndex(elementType);
        ArgumentOutOfRangeException.ThrowIfNegative(elementNumber);
        lock (syncLock)
        {
            var resultSet = new HashSet<(int Type, int Node)>();
            for (var nt = 0; nt < NumberOfTypes; nt++)
                if (elementNumber < mat[elementType, nt].Count)
                    resultSet.UnionWith(mat[elementType, nt][elementNumber]
                        .Select(node => (nt, node)));
            return resultSet.OrderBy(x => x).ToList();
        }
    }

    public List<(int Type, int Node)> GetAllNodes(int elementType)
    {
        ValidateTypeIndex(elementType);
        lock (syncLock)
        {
            var result = new List<(int Type, int Node)>();
            for (var e = 0; e < GetNumberOfElements(elementType); e++)
            {
                var nodes = GetAllNodes(elementType, e);
                foreach (var node in nodes)
                    if (!result.Contains(node))
                        result.Add(node);
            }

            result.Sort((x, y) => x.CompareTo(y));
            return result;
        }
    }

    public List<(int ElemType, int Elem)> DepthFirstSearchFromANode(int nodeType, int node)
    {
        ValidateTypeIndex(nodeType);
        ArgumentOutOfRangeException.ThrowIfNegative(node);
        lock (syncLock)
        {
            var visited = new HashSet<(int ElemType, int Elem)>();
            var stack = new Stack<(int ElemType, int Elem)>();
            stack.Push((nodeType, node));
            while (stack.Count > 0)
            {
                var curr = stack.Pop();
                if (!visited.Add(curr)) continue;
                foreach (var e in GetAllElements(curr.ElemType, curr.Elem))
                    stack.Push(e);
            }

            return visited.OrderBy(x => x).ToList();
        }
    }

    public int AppendElement(int elementType, int nodeType, List<int> nodes)
    {
        ValidateTypeIndex(elementType);
        ValidateTypeIndex(nodeType);
        ArgumentNullException.ThrowIfNull(nodes);
        lock (syncLock)
        {
            return mat[elementType, nodeType].AppendElement(nodes);
        }
    }

    public void Compress()
    {
        if (!ListOfMarked.Any(kv => kv.Value.Count > 0)) return;

        lock (syncLock)
        {
            List<(List<int> newNodesFromOld, List<int> oldNodesFromNew)> remap = new();
            for (var type = 0; type < NumberOfTypes; type++)
            {
                var listOfElementsToKill = ListOfMarked[type].ToList();
                var numberOfNodesFromType = mat[type, type].Count;
                remap.Add(Utils.GetNodeMapsFromKillList(numberOfNodesFromType, listOfElementsToKill));
            }

            for (var elementtype = 0; elementtype < NumberOfTypes; ++elementtype)
            {
                var emap = remap[elementtype];
                var oldElementsFromNew = emap.oldNodesFromNew;
                for (var nodetype = 0; nodetype < NumberOfTypes; ++nodetype)
                {
                    var nmap = remap[nodetype];
                    var newNodesFromOld = nmap.newNodesFromOld;
                    mat[elementtype, nodetype].RearrangeAfterRenumbering(oldElementsFromNew, newNodesFromOld);
                }
            }

            for (var i = 0; i < ListOfMarked.Count; ++i) ListOfMarked[i].Clear();
        }
    }

    public List<int> GetTypeTopOrder()
    {
        lock (syncLock)
        {
            var typeDeps = new O2M(NumberOfTypes);
            for (var e = 0; e < NumberOfTypes; e++)
                typeDeps.AppendElement([]);
            var hasDeps = false;
            for (var e = 0; e < NumberOfTypes; e++)
            for (var n = 0; n < NumberOfTypes; n++)
                if (n != e && mat[e, n].Count > 0)
                {
                    typeDeps.AppendNodeToElement(e, n);
                    hasDeps = true;
                }

            return hasDeps ? typeDeps.GetTopOrder() : Enumerable.Range(0, NumberOfTypes).ToList();
        }
    }

    public bool AreTypesAcyclic()
    {
        lock (syncLock)
        {
            var typeDeps = new O2M(NumberOfTypes);
            for (var e = 0; e < NumberOfTypes; e++)
                typeDeps.AppendElement([]);
            var hasDeps = false;
            for (var e = 0; e < NumberOfTypes; e++)
            for (var n = 0; n < NumberOfTypes; n++)
                if (n != e && mat[e, n].Count > 0)
                {
                    typeDeps.AppendNodeToElement(e, n);
                    hasDeps = true;
                }

            return hasDeps ? typeDeps.IsAcyclic() : true;
        }
    }

    public List<int> GetElementsFromNodes(int elementType, int nodeType, List<int> nodes)
    {
        ValidateTypeIndex(elementType);
        ValidateTypeIndex(nodeType);
        ArgumentNullException.ThrowIfNull(nodes);
        lock (syncLock)
        {
            return mat[elementType, nodeType].GetElementsFromNodes(nodes);
        }
    }

    public List<int> GetElementsWithNodes(int elementType, int nodeType, List<int> nodes)
    {
        ValidateTypeIndex(elementType);
        ValidateTypeIndex(nodeType);
        ArgumentNullException.ThrowIfNull(nodes);
        lock (syncLock)
        {
            return mat[elementType, nodeType].GetElementsWithNodes(nodes);
        }
    }

    private void ValidateTypeIndex(int typeIndex)
    {
        if (typeIndex < 0 || typeIndex >= NumberOfTypes)
            throw new ArgumentOutOfRangeException(nameof(typeIndex), "Type index is out of range.");
    }

    private void InitializeMarkedCollections()
    {
        for (var i = 0; i < NumberOfTypes; i++) 
            ListOfMarked[i] = new HashSet<int>();
    }

    private void MarkNodeForErasure(int nodeType, int node)
    {
        lock (syncLock)
        {
            ListOfMarked[nodeType].Add(node);
        }
    }

    private void MarkDependentElementsForErasure(List<(int ElemType, int Elem)> dependentElements)
    {
        foreach (var pair in dependentElements)
            MarkToErase(pair.ElemType, pair.Elem);
    }
}
