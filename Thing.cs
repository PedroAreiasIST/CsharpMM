namespace mm2;

public class ThingModel
{
    public Dictionary<int, Dictionary<int, List<List<int>>>> ChildrenBuilders { get; } =
        new(); // [childtype][nodetype][localnode]

    public Dictionary<int, List<List<int>>> SymmetryGroups { get; } =
        new(); // [nodetype][combinationnumber][localnodes]

    public static void AppendSymmetryGroup(List<ThingModel> model, int ElementType, int NodeType,
        List<List<int>> group)
    {
        if (ElementType >= model.Count)
            model.Add(new ThingModel());

        if (!model[ElementType].SymmetryGroups.ContainsKey(NodeType))
            model[ElementType].SymmetryGroups[NodeType] = new List<List<int>>();
        model[ElementType].SymmetryGroups[NodeType] = group;
    }

    public static void AppendChildrenBuilder(List<ThingModel> model, int ElementType,
        int ChildType, int NodeType, List<int> localNodesInEm)
    {
        if (ElementType >= model.Count)
            model.Add(new ThingModel());

        if (!model[ElementType].ChildrenBuilders.ContainsKey(ChildType))
            model[ElementType].ChildrenBuilders[ChildType] = new Dictionary<int, List<List<int>>>();

        if (!model[ElementType].ChildrenBuilders[ChildType].ContainsKey(NodeType))
            model[ElementType].ChildrenBuilders[ChildType][NodeType] = new List<List<int>>();

        model[ElementType].ChildrenBuilders[ChildType][NodeType].Add(localNodesInEm);
    }
}

public class Thing
{
    public Dictionary<int, List<int>> TypesAndNodes { get; set; } = new(); // [nodetype][localnode]->nodenumber
    public int Typenumber { get; set; } = -1;

    // FIX: Changed from trying to index into non-existent positions to simply assigning the list
    private void AppendElement(int nodetype, List<int> nodes)
    {
        if (!TypesAndNodes.ContainsKey(nodetype))
            TypesAndNodes[nodetype] = new List<int>();

        // Simply assign the nodes list instead of trying to index
        TypesAndNodes[nodetype] = nodes;
    }

    public List<Thing> GetAllChildren(ThingModel model)
    {
        var result = new List<Thing>();
        // Loop over possible child types.
        for (var childtype = 0;
             childtype < model.ChildrenBuilders.Count;
             ++childtype)
            // Only process if there is at least one children builder for this child
            // type.
            if (model.ChildrenBuilders[childtype].Count != 0)
                // Loop over each nodetype available for this child type.
                for (var nodetype = 0;
                     nodetype < model.ChildrenBuilders[childtype].Count;
                     ++nodetype)
                {
                    var builderCollection = model.ChildrenBuilders[childtype][nodetype];
                    // For each instance in the builder for the current nodetype, create a
                    // new child.
                    for (var i = 0; i < builderCollection.Count; ++i)
                    {
                        var localnodes = builderCollection[i];
                        // Retrieve the nodes from the parent 'element' based on these
                        // indices.
                        var nodes = TypesAndNodes[nodetype].This(localnodes);
                        // Create a new child object.
                        var child = new Thing
                        {
                            Typenumber = childtype
                        };
                        child.AppendElement(nodetype, nodes);
                        result.Add(child);
                    }
                }

        return result;
    }

    public void UploadThing(MM2M m, Thing e, ThingModel model)
    {
        for (var i = 0; i < TypesAndNodes.Count; ++i)
        {
            var canon = Symmetries.GetCanonicalForm(TypesAndNodes[i], model.SymmetryGroups[i]);
            m.AppendElement(e.Typenumber, i, canon);
        }
    }
}
