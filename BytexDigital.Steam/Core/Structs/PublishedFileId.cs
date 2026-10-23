namespace BytexDigital.Steam.Core.Structs
{
    public readonly record struct PublishedFileId(ulong Id)
    {
        public static implicit operator PublishedFileId(ulong id) => new PublishedFileId(id);

        public static implicit operator ulong(PublishedFileId publishedFileId) => publishedFileId.Id;

        public override string ToString() => Id.ToString();
    }
}