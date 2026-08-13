using System.Globalization;

namespace B3.Umdf.FixConflated;

public sealed class FixMessage
{
    private readonly List<FixField> _fields;

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

    public FixMessage(IEnumerable<FixField> fields)
    {
        _fields = new List<FixField>(fields);
    }

    public IReadOnlyList<FixField> Fields => _fields;

    public void Add(int tag, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        _fields.Add(new FixField(tag, value));
    }

    public void Add(int tag, int value) => Add(tag, value.ToString(CultureInfo.InvariantCulture));

    public void AddBoolean(int tag, bool value) => Add(tag, value ? "Y" : "N");

    public bool RemoveAll(int tag)
    {
        int removed = _fields.RemoveAll(f => f.Tag == tag);
        return removed != 0;
    }

    public void Upsert(int tag, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
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

    public FixMessage Clone() => new(_fields);
}
