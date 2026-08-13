using System.Globalization;

namespace B3.Umdf.FixConflated;

public sealed class FixMessage : IReadOnlyList<FixField>
{
    private readonly List<FixField> _fields;
    private FixMessage? _appendedFields;

    public FixMessage()
    {
        _fields = new List<FixField>();
    }

    public FixMessage(int capacity)
    {
        if (capacity < 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        _fields = new List<FixField>(capacity);
    }

    public FixMessage(FixMessage appendedFields, int capacity = 0)
    {
        ArgumentNullException.ThrowIfNull(appendedFields);
        if (capacity < 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        _fields = new List<FixField>(capacity);
        _appendedFields = appendedFields;
    }

    public FixMessage(IEnumerable<FixField> fields)
    {
        _fields = new List<FixField>(fields);
    }

    public IReadOnlyList<FixField> Fields => this;
    public int Count => _fields.Count + (_appendedFields?.Count ?? 0);

    public FixField this[int index]
    {
        get
        {
            if ((uint)index < (uint)_fields.Count)
                return _fields[index];

            if (_appendedFields is null)
                throw new ArgumentOutOfRangeException(nameof(index));

            int appendedIndex = index - _fields.Count;
            if ((uint)appendedIndex < (uint)_appendedFields.Count)
                return _appendedFields[appendedIndex];

            throw new ArgumentOutOfRangeException(nameof(index));
        }
    }

    public void Add(int tag, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        _fields.Add(new FixField(tag, value));
    }

    public void Add(int tag, int value) => Add(tag, value.ToString(CultureInfo.InvariantCulture));

    public void AddBoolean(int tag, bool value) => Add(tag, value ? "Y" : "N");

    public bool RemoveAll(int tag)
    {
        EnsureWritable();
        int removed = _fields.RemoveAll(f => f.Tag == tag);
        return removed != 0;
    }

    public void Upsert(int tag, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        EnsureWritable();
        for (int i = 0; i < _fields.Count; i++)
        {
            if (_fields[i].Tag != tag)
                continue;

            _fields[i] = new FixField(tag, value);
            return;
        }

        _fields.Add(new FixField(tag, value));
    }

    public bool TryGetString(int tag, out string? value)
    {
        for (int i = 0; i < _fields.Count; i++)
        {
            if (_fields[i].Tag != tag)
                continue;

            value = _fields[i].Value;
            return true;
        }

        if (_appendedFields is not null)
            return _appendedFields.TryGetString(tag, out value);

        value = null;
        return false;
    }

    public bool TryGetInt32(int tag, out int value)
    {
        if (TryGetString(tag, out var raw) &&
            int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
            return true;

        value = 0;
        return false;
    }

    public bool TryGetBoolean(int tag, out bool value)
    {
        if (TryGetString(tag, out var raw))
        {
            if (string.Equals(raw, "Y", StringComparison.Ordinal))
            {
                value = true;
                return true;
            }

            if (string.Equals(raw, "N", StringComparison.Ordinal))
            {
                value = false;
                return true;
            }
        }

        value = false;
        return false;
    }

    public FixMessage Clone()
    {
        var clone = new FixMessage(Count);
        for (int i = 0; i < _fields.Count; i++)
            clone._fields.Add(_fields[i]);

        if (_appendedFields is not null)
        {
            for (int i = 0; i < _appendedFields.Count; i++)
                clone._fields.Add(_appendedFields[i]);
        }

        return clone;
    }

    private void EnsureWritable()
    {
        if (_appendedFields is null)
            return;

        for (int i = 0; i < _appendedFields.Count; i++)
            _fields.Add(_appendedFields[i]);

        _appendedFields = null;
    }

    public IEnumerator<FixField> GetEnumerator()
    {
        for (int i = 0; i < _fields.Count; i++)
            yield return _fields[i];

        if (_appendedFields is not null)
        {
            for (int i = 0; i < _appendedFields.Count; i++)
                yield return _appendedFields[i];
        }
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        => GetEnumerator();
}
