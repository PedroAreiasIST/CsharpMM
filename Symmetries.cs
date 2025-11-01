namespace mm2;

using Group = List<List<int>>;

internal static class Constants
{
    public static readonly int UNSET = -1;
}

/// <summary>
///     Provides symmetry-related operations for permutation groups.
/// </summary>
public static class Symmetries
{
    /// <summary>
    ///     Checks if the given position is a valid permutation that exists in the group.
    /// </summary>
    /// <param name="position">The permutation to validate.</param>
    /// <param name="group">The group of permutations to check against.</param>
    /// <returns>True if the permutation is valid within the group, false otherwise.</returns>
    private static bool IsValidFullPermutation(List<int> position, Group group)
    {
        ArgumentNullException.ThrowIfNull(position);
        ArgumentNullException.ThrowIfNull(group);

        foreach (var perm in group)
        {
            if (perm.Count != position.Count)
                continue;

            if (perm.SequenceEqual(position))
                return true; // Found a matching permutation in the group
        }

        return false;
    }

    /// <summary>
    ///     Checks if the current partial position can be extended to a valid permutation.
    /// </summary>
    /// <param name="position">The current partial position.</param>
    /// <param name="upTo">The index up to which to check for consistency.</param>
    /// <param name="group">The group of permutations to check against.</param>
    /// <returns>True if the position can be extended, false otherwise.</returns>
    private static bool CanExtend(List<int> position, int upTo, Group group)
    {
        ArgumentNullException.ThrowIfNull(position);
        ArgumentNullException.ThrowIfNull(group);

        var positionSize = position.Count;

        foreach (var perm in group)
        {
            if (perm.Count != positionSize)
                continue;

            var match = true;
            // Check consistency up to 'upTo' index
            for (var k = 0; k <= upTo; ++k)
                if (position[k] != Constants.UNSET && position[k] != perm[k])
                {
                    match = false;
                    break;
                }

            if (match)
                return true; // At least one permutation is consistent so far
        }

        return false;
    }

    /// <summary>
    ///     Recursively assigns indices to positions to find a valid permutation in the group.
    /// </summary>
    /// <param name="level">The current recursion level (position index being assigned).</param>
    /// <param name="position">The current permutation being built.</param>
    /// <param name="sortedIndices">Indices sorted by label values.</param>
    /// <param name="used">Tracks which indices have been used.</param>
    /// <param name="group">The group of permutations to check against.</param>
    /// <returns>True if a valid assignment was found, false otherwise.</returns>
    private static bool AssignSlot(int level, List<int> position, List<int> sortedIndices, List<bool> used, Group group)
    {
        ArgumentNullException.ThrowIfNull(position);
        ArgumentNullException.ThrowIfNull(sortedIndices);
        ArgumentNullException.ThrowIfNull(used);
        ArgumentNullException.ThrowIfNull(group);

        var n = position.Count;
        // If all slots are assigned, check if the permutation is valid in the group
        if (level == n)
            return IsValidFullPermutation(position, group);

        // Try each candidate index (in sorted order) that has not been used yet.
        foreach (var idx in sortedIndices)
            if (!used[idx])
            {
                position[level] = idx;
                if (CanExtend(position, level, group))
                {
                    used[idx] = true;
                    try
                    {
                        if (AssignSlot(level + 1, position, sortedIndices, used, group))
                            return true;
                    }
                    finally
                    {
                        // Ensure we always reset the used flag, even if an exception is thrown
                        used[idx] = false;
                    }
                }

                // Reset the position slot if assignment failed
                position[level] = Constants.UNSET;
            }

        return false;
    }

    /// <summary>
    ///     Gets the canonical form of the given labels with respect to a symmetry group.
    /// </summary>
    /// <param name="labels">The labels to canonicalize.</param>
    /// <param name="group">The symmetry group to use for canonicalization.</param>
    /// <returns>A new list containing the canonical form of the labels.</returns>
    /// <exception cref="ArgumentNullException">Thrown if labels or group is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if no valid labeling can be found for the group.</exception>
    public static List<int> GetCanonicalForm(List<int> labels, Group group)
    {
        ArgumentNullException.ThrowIfNull(labels);
        ArgumentNullException.ThrowIfNull(group);

        var n = labels.Count;

        // If the group is empty or the permutation sizes do not match, return the original labels.
        if (group.Count == 0 || group[0].Count != n)
            return [..labels];

        // (1) Build sortedIndices sorted by corresponding label values in ascending order.
        var sortedIndices = Enumerable.Range(0, n).ToList();
        sortedIndices.Sort((a, b) => labels[a].CompareTo(labels[b]));

        // (2) Initialize position with UNSET and a boolean array for used indices.
        var position = Enumerable.Repeat(Constants.UNSET, n).ToList();
        var used = Enumerable.Repeat(false, n).ToList();

        // (3) Recursively assign slots to obtain a canonical permutation.
        if (!AssignSlot(0, position, sortedIndices, used, group))
            throw new InvalidOperationException("No valid labeling found for the group.");

        // Build the final canonical labeling by mapping indices to original labels.
        var canonical = new List<int>(n);
        for (var i = 0; i < n; i++)
            canonical.Add(labels[position[i]]);

        return canonical;
    }
}