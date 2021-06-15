// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Threading
{
    /// <summary>
    /// Queue for executing asynchronous tasks in a first-in-first-out fashion per table.
    /// </summary>
    internal class ActionBlock : IDisposable
    {
        private readonly AsyncMultiReaderSingleWriterDictionary tableLocks = new AsyncMultiReaderSingleWriterDictionary();

        public async Task PostReadOnly(string table, Func<Task> action, CancellationToken cancellationToken)
        {
            using (await tableLocks.AcquireReadLock(table, cancellationToken))
            {
                await action();
            }
        }

        public async Task Post(Func<Task> action, CancellationToken cancellationToken)
        {
            using (await tableLocks.AcquireWriteLock(cancellationToken))
            {
                await action();
            }
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
                this.tableLocks.Dispose();
            }
        }

        public virtual Task<IDisposable> LockTableAsync(string name, CancellationToken cancellationToken)
        {
            return this.tableLocks.AcquireReadLock(name, cancellationToken);
        }
    }
}