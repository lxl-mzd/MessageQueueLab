using MessageQueueLab.Core;
using MessageQueueLab.Models;
using Xunit;

namespace MessageQueueLab.Tests;

public class SnapshotTests
{
    [Fact]
    public void ApplySnapshot_ShouldRestoreQueues()
    {
        var dir1 = Path.Combine(Path.GetTempPath(), "mqlab-tests-snap-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir1);
        var hub1 = new MessageQueueHub(dir1);
        var q1 = hub1.Get("normal")!;
        q1.Send("alpha");
        q1.Send("beta");
        var snap = hub1.CaptureSnapshot();

        var dir2 = Path.Combine(Path.GetTempPath(), "mqlab-tests-snap2-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir2);
        var hub2 = new MessageQueueHub(dir2);
        hub2.ApplySnapshot(snap);

        var q2 = hub2.Get("normal")!;
        var recv = q2.Receive();
        Assert.NotNull(recv);
        Assert.Equal("alpha", recv!.Content);
        Directory.Delete(dir1, true);
        Directory.Delete(dir2, true);
    }
}