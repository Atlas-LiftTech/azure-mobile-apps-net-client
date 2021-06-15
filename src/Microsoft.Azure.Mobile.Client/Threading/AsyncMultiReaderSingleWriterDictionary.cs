using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Threading
{
    internal class AsyncMultiReaderSingleWriterDictionary : IDisposable
    {
        private sealed class LockEntry : IDisposable
        {
            public int Count;
            public readonly AsyncLock Lock = new AsyncLock();

            public void Dispose()
            {
                this.Lock.Dispose();
            }
        }

        private readonly Dictionary<string, LockEntry> readerLocks = new Dictionary<string, LockEntry>();
        private readonly ManualResetEventSlim waitingWriterLock = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim waitingReaderLock = new ManualResetEventSlim(true);

        private int readersWaiting;
        private int writersWaiting;

        public async Task<IDisposable> AcquireWriteLock(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref writersWaiting);

            waitingWriterLock.Wait(cancellationToken);

            // Hold any future reads until we are done writing
            waitingReaderLock.Reset();

            return new DisposeAction(() =>
            {
                Interlocked.Decrement(ref writersWaiting);

                if (writersWaiting == 0)
                {
                    waitingWriterLock.Reset();

                    // Allow reads again
                    waitingReaderLock.Set();
                }
            });
        }

        public async Task<IDisposable> AcquireReadLock(string key, CancellationToken cancellationToken)
        {
            LockEntry entry;

            // Wait until writes are done
            waitingReaderLock.Wait(cancellationToken);

            lock (readerLocks)
            {
                if (!readerLocks.TryGetValue(key, out entry))
                {
                    readerLocks[key] = entry = new LockEntry();
                }
                ++entry.Count;
                ++readersWaiting;
            }

            IDisposable releaser = await entry.Lock.Acquire(cancellationToken);

            return new DisposeAction(() =>
            {
                lock (readerLocks)
                {
                    --entry.Count;
                    --readersWaiting;

                    releaser.Dispose();

                    if (entry.Count == 0)
                    {
                        this.readerLocks.Remove(key);
                        entry.Dispose();
                    }

                    if (writersWaiting > 0 && readersWaiting == 0)
                    {
                        // Allow writes once all readers are done
                        waitingReaderLock.Reset();
                        waitingWriterLock.Set();
                    }
                }
            });
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (readerLocks)
                {
                    waitingWriterLock.Dispose();
                    waitingReaderLock.Dispose();

                    foreach (var l in readerLocks.Values)
                    {
                        l.Dispose();
                    }
                }
            }
        }
    }
}