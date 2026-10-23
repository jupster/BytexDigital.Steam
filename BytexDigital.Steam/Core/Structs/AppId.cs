namespace BytexDigital.Steam.Core.Structs
{
    public readonly record struct AppId(uint Id)
    {
        public static implicit operator AppId(uint id) => new AppId(id);

        public static implicit operator uint(AppId appId) => appId.Id;

        public override string ToString() => Id.ToString();
    }
}