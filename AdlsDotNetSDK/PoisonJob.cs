using System;
namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Poison job that is enqueued in the priority queue when the enumeration by the producer thread is completed. 
    /// The priority of the poison job is negative so it will always be polled at end when no jobs are left.
    /// </summary>
    internal class PoisonJob : BaseJob
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

        protected override object DoJob()
        {
            throw new NotImplementedException();
        }
    }
}
