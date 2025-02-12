﻿// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    /// <summary>
    /// Base class for all sync actions i.e. Pull, Purge and Push
    /// </summary>
    internal abstract class SyncAction
    {
        protected OperationQueue OperationQueue { get; private set; }
        protected TaskCompletionSource<int> TaskSource { get; private set; }
        protected IMobileServiceLocalStore Store { get; private set; }
        public CancellationToken CancellationToken { get; private set; }

        public Task CompletionTask => TaskSource.Task;

        public SyncAction(OperationQueue operationQueue, IMobileServiceLocalStore store, CancellationToken cancellationToken)
        {
            this.OperationQueue = operationQueue;
            this.Store = store;
            this.TaskSource = new TaskCompletionSource<int>();
            this.CancellationToken = cancellationToken;

            cancellationToken.Register(() => TaskSource.TrySetCanceled());
        }

        public abstract Task ExecuteAsync();
    }
}
