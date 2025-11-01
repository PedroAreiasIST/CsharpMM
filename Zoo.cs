namespace mm2;

public class Zoo
{
    private readonly MM2M lifemanager;
    private readonly TypeManager mesh;
    private readonly List<ThingModel> models;
    private readonly MM2M synthesized;

    public Zoo(TypeManager amesh)
    {
        mesh = amesh;
        lifemanager = new MM2M(amesh.Count);
        synthesized = new MM2M(amesh.Count);
        models = new List<ThingModel>(amesh.Count);
    }

    public void AppendSymmetryGroup<E, N>(List<List<int>> group)
    {
        var elementType = mesh.GetTypeIndex<E>();
        var nodeType = mesh.GetTypeIndex<N>();
        if (elementType >= models.Count)
            models.Add(new ThingModel());
        ThingModel.AppendSymmetryGroup(models, elementType, nodeType, group);
    }

    public void AppendChildrenBuilder<E, C, N>(List<int> localNodesInEm)
    {
        var elementType = mesh.GetTypeIndex<E>();
        var childType = mesh.GetTypeIndex<C>();
        var nodeType = mesh.GetTypeIndex<N>();
        if (elementType >= models.Count)
            models.Add(new ThingModel());
        ThingModel.AppendChildrenBuilder(models, elementType, childType, nodeType, localNodesInEm);
    }

    public int AppendNode<N>(params object[] args)
    {
        return mesh.AppendNode<N>(args);
    }

    public void AppendNodesToaThing<E, N>(Thing athing, List<int> nodenumbers)
    {
        var elementType = mesh.GetTypeIndex<E>();
        var nodeType = mesh.GetTypeIndex<N>();
        athing.Typenumber = elementType;
        athing.TypesAndNodes[nodeType] = nodenumbers;
    }

    private void uploadelement(Thing defined)
    {
        defined.UploadThing(lifemanager, defined, models[defined.Typenumber]);
        var children = defined.GetAllChildren(models[defined.Typenumber]);
    }
}