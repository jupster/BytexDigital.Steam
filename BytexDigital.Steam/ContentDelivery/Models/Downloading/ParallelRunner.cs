namespace BytexDigital.Steam.ContentDelivery.Models.Downloading
{
    internal static class ParallelRunner
    {
        /// <summary>
        ///     Runs the tasks with at most maxParallel at a time. If one fails or the caller cancels,
        ///     the others are cancelled and awaited, so nothing is left running or unobserved.
        /// </summary>
        public static async Task RunAsync(
            int maxParallel,
            IEnumerable<Func<CancellationToken, Task>> taskFactories,
            CancellationToken cancellationToken)
        {
            var taskFactoriesSource = taskFactories.ToArray();

            if (taskFactoriesSource.Length == 0)
            {
                return;
            }

            if (maxParallel <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxParallel));
            }

            // Cancelling this stops every running task without cancelling the caller's own token
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            var tasksRunning = new List<Task>(50);
            var index = 0;

            try
            {
                do
                {
                    linked.Token.ThrowIfCancellationRequested();

                    while (tasksRunning.Count < maxParallel && index < taskFactoriesSource.Length)
                    {
                        var taskFactory = taskFactoriesSource[index++];

                        tasksRunning.Add(taskFactory(linked.Token));
                    }

                    var completedTask = await Task.WhenAny(tasksRunning).ConfigureAwait(false);

                    await completedTask.ConfigureAwait(false);

                    tasksRunning.Remove(completedTask);
                } while (index < taskFactoriesSource.Length || tasksRunning.Count != 0);
            }
            catch
            {
                // Stop everything still running and wait for it to finish
                linked.Cancel();

                try
                {
                    await Task.WhenAll(tasksRunning).ConfigureAwait(false);
                }
                catch
                {
                    // These are the follow-on cancellations. The first failure is rethrown below.
                }

                throw;
            }
        }
    }
}