using System;
namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Poison job that is enqueued in the priority queue when the enumeration by the producer thread is completed. 
    /// The priority of the poison job is negative so it will always be polled at end when no jobs are left.
    /// </summary>
    internal class PoisonJob : Job
    {
        internal PoisonJob() : base(-1)
        {
        }

        protected override string JobDetails()
        {
            throw new NotImplementedException();
        }

        protected override string JobType()
        {
            throw new NotImplementedException();
        }

        protected override SingleEntryTransferStatus DoJob()
        {
            throw new NotImplementedException();
        }
    }
}
