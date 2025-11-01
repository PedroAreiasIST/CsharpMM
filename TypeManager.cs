using System.Collections;
using System.Text;

namespace mm2;

/// <summary>
///     Manages type registration and provides type-safe access to collections of objects.
/// </summary>
public class TypeManager
{
    private readonly TypeRegistry _registry;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeManager" /> class with the specified types.
    /// </summary>
    /// <param name="types">The types to be managed. None of the types can be null, and duplicates are not allowed.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="types" /> is null.</exception>
    /// <exception cref="ArgumentException">If <paramref name="types" /> contains a null element or a duplicate type.</exception>
    public TypeManager(params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(types);

        _registry = new TypeRegistry(types);
    }

    /// <summary>
    ///     Gets the number of registered types.
    /// </summary>
    public int Count => _registry.TypeCount;

    /// <summary>
    ///     Retrieves the sequence (list) of objects for the specified type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type of objects in the sequence.</typeparam>
    /// <returns>A <see cref="List{T}" /> containing objects of type <typeparamref name="T" />.</returns>
    /// <exception cref="InvalidOperationException">If type <typeparamref name="T" /> is not registered.</exception>
    public List<T> GetSequence<T>()
    {
        return _registry.GetSequenceOfType<T>();
    }

    /// <summary>
    ///     Gets the zero-based index of the registered type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type whose index is to be retrieved.</typeparam>
    /// <returns>The zero-based index of the type.</returns>
    /// <exception cref="InvalidOperationException">If type <typeparamref name="T" /> is not registered.</exception>
    public int GetTypeIndex<T>()
    {
        return _registry.GetIndexOfType<T>();
    }

    /// <summary>
    ///     Creates an instance of type <typeparamref name="T" /> using the provided constructor arguments
    ///     and appends it to the corresponding sequence.
    /// </summary>
    /// <typeparam name="T">The type of object to create and append.</typeparam>
    /// <param name="constructorArgs">The arguments to pass to the constructor of <typeparamref name="T" />.</param>
    /// <returns>The index of the newly added node within its type sequence.</returns>
    /// <exception cref="InvalidOperationException">
    ///     If type <typeparamref name="T" /> is not registered,
    ///     or if an instance cannot be created (e.g., no matching constructor, constructor throws an exception).
    /// </exception>
    public int AppendNode<T>(params object[] constructorArgs)
    {
        var sequence = GetSequence<T>(); // Ensures type is registered
        T instance;

        try
        {
            instance = (T)Activator.CreateInstance(typeof(T), constructorArgs);
            if (instance == null)
                // This case is unlikely with Activator.CreateInstance for non-nullable reference types
                // or value types, but good for robustness with potentially nullable T.
                throw new InvalidOperationException($"Activator.CreateInstance returned null for type {typeof(T)}.");
        }
        catch (MissingMethodException ex)
        {
            throw new InvalidOperationException(
                $"No constructor found for type {typeof(T)} with the given arguments.", ex);
        }
        // Catching a broad Exception and re-throwing as InvalidOperationException
        // might obscure the original error. Consider if specific exceptions should be handled
        // or if the original exception should be allowed to propagate in some cases.
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to create instance of {typeof(T)}.", ex);
        }

        sequence.Add(instance);
        return sequence.Count - 1;
    }

    /// <summary>
    ///     Appends an element by invoking a provided function with type indices.
    ///     This method allows for flexible element addition based on relationships between registered types.
    /// </summary>
    /// <typeparam name="E">The 'element' type.</typeparam>
    /// <typeparam name="N">The 'node' type.</typeparam>
    /// <param name="appendFunc">
    ///     A function that takes the index of type <typeparamref name="E" />,
    ///     the index of type <typeparamref name="N" />, and an optional collection of integer fields,
    ///     and returns an integer (presumably an index or status).
    /// </param>
    /// <param name="fields">
    ///     Optional collection of integer fields to pass to <paramref name="appendFunc" />. Defaults to an
    ///     empty collection if null.
    /// </param>
    /// <returns>The result of the <paramref name="appendFunc" /> delegate.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="appendFunc" /> is null.</exception>
    /// <exception cref="InvalidOperationException">
    ///     If type <typeparamref name="E" /> or <typeparamref name="N" /> is not
    ///     registered.
    /// </exception>
    public int AppendElement<E, N>(
        Func<int, int, IEnumerable<int>, int> appendFunc,
        IEnumerable<int>? fields = null)
    {
        ArgumentNullException.ThrowIfNull(appendFunc);

        var elementIndex = GetTypeIndex<E>(); // Ensures E is registered
        var nodeIndex = GetTypeIndex<N>(); // Ensures N is registered

        return appendFunc(elementIndex, nodeIndex, fields ?? []);
    }

    /// <summary>
    ///     Returns a string representation of all items in all managed sequences.
    ///     Items are converted to strings and separated by spaces.
    /// </summary>
    /// <returns>A string representing all items.</returns>
    public override string ToString()
    {
        return _registry.GetAllItemsAsString();
    }

    /// <summary>
    ///     Manages the registration and storage of types and their associated collections.
    ///     This class encapsulates the underlying data structures and logic for type management.
    /// </summary>
    private class TypeRegistry
    {
        private readonly List<Type> _registeredTypes = [];
        private readonly Dictionary<Type, object> _sequences = [];

        public TypeRegistry(Type[] types)
        {
            for (var i = 0; i < types.Length; i++)
            {
                var type = types[i];
                if (type == null)
                    throw new ArgumentException($"The type at index {i} in the input array cannot be null.",
                        nameof(types));

                if (_sequences.ContainsKey(type))
                    throw new ArgumentException($"Type {type.FullName} is already registered.", nameof(types));

                _registeredTypes.Add(type);
                CreateSequenceForType(type);
            }
        }

        public int TypeCount => _registeredTypes.Count;

        private void CreateSequenceForType(Type type)
        {
            var listType = typeof(List<>).MakeGenericType(type);
            var sequenceInstance = Activator.CreateInstance(listType);

            if (sequenceInstance == null)
                // This should ideally not happen for List<T>
                throw new InvalidOperationException(
                    $"Failed to create list instance for type {type.FullName}. Activator.CreateInstance returned null.");
            _sequences[type] = sequenceInstance;
        }

        public List<T> GetSequenceOfType<T>()
        {
            var typeOfT = typeof(T);
            if (!_sequences.TryGetValue(typeOfT, out var sequenceObject))
                throw new InvalidOperationException($"Type {typeOfT.FullName} is not registered.");

            // This cast should be safe due to the way sequences are created.
            // However, including a try-catch or a more robust check can be considered
            // if there's any doubt about the integrity of _sequences.
            try
            {
                return (List<T>)sequenceObject;
            }
            catch (InvalidCastException ex)
            {
                // This would indicate an internal logic error if it occurs.
                throw new InvalidOperationException(
                    $"Internal error: Type mismatch when retrieving sequence for {typeOfT.FullName}. Expected List<{typeOfT.FullName}> but found {sequenceObject.GetType().FullName}.",
                    ex);
            }
        }

        public int GetIndexOfType<T>()
        {
            var typeOfT = typeof(T);
            var index = _registeredTypes.IndexOf(typeOfT);

            if (index < 0) throw new InvalidOperationException($"Type {typeOfT.FullName} is not registered.");
            return index;
        }

        public string GetAllItemsAsString()
        {
            var sb = new StringBuilder();
            foreach (var type in _registeredTypes)
                // Ensured by constructor that type exists in _sequences
                if (_sequences[type] is IEnumerable sequence)
                    foreach (var item in sequence)
                        sb.Append(item?.ToString() ?? "null").Append(' ');

            // Remove trailing space if any items were added
            if (sb.Length > 0) sb.Length--;
            return sb.ToString();
        }
    }
}