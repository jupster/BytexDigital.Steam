using BytexDigital.Steam.ContentDelivery.Models.Downloading;

namespace BytexDigital.Steam.Tests.ContentDelivery.Models.Downloading;

public class DownloadPathHelperTests
{
    private static readonly string Root = Path.Combine(Path.GetTempPath(), "dl");

    [Theory]
    [InlineData("file.txt")]
    [InlineData("sub/dir/file.txt")]
    public void FilesInsideTheDirectoryAreAllowed(string name)
    {
        var path = DownloadPathHelper.GetSafePath(Root, name);

        Assert.StartsWith(Path.GetFullPath(Root), path);
    }

    [Theory]
    [InlineData("../evil.txt")]
    [InlineData("sub/../../evil.txt")]
    [InlineData("../dl-evil/x.txt")]
    [InlineData("..")]
    [InlineData(".")]
    public void NamesThatEscapeOrEqualTheDirectoryAreRejected(string name)
    {
        Assert.Throws<InvalidDataException>(() => DownloadPathHelper.GetSafePath(Root, name));
    }

    [Fact]
    public void RootedNamesAreRejected()
    {
        var rooted = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "other", "x.txt"));

        Assert.Throws<InvalidDataException>(() => DownloadPathHelper.GetSafePath(Root, rooted));
    }
}