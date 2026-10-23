namespace BytexDigital.Steam.Core.Structs
{
    public readonly record struct ManifestId(ulong Id)
    {
        public static implicit operator ManifestId(ulong id) => new ManifestId(id);

        public static implicit operator ulong(ManifestId manifestId) => manifestId.Id;

        public override string ToString() => Id.ToString();
    }
}