namespace BytexDigital.Steam.Core.Structs
{
    public readonly record struct DepotId(uint Id)
    {
        public static implicit operator DepotId(uint id) => new DepotId(id);

        public static implicit operator uint(DepotId depotId) => depotId.Id;

        public static implicit operator DepotId(AppId appId) => new DepotId(appId.Id);

        public override string ToString() => Id.ToString();
    }
}