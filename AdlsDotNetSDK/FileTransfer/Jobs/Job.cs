using System;
using System.Diagnostics;
using NLog;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Abstract class for a job
    /// </summary>
    internal abstract class Job : IComparable
    {
        private static readonly Logger JobLog = LogManager.GetLogger("adls.dotnet.FileTransfer.Job");
        /// <summary>
        /// Priority- Represents it's position in the max-heap. Higher priority means the job will be completed quicker.
        /// ChunkedFileCopy has higher priority than NonChunkedFile copy and concatenate job
        /// </summary>
        protected long JobPriority { get; }

        protected Job(long priority)
        {
            JobPriority = priority;
        }
        public int CompareTo(object obj)
        {
            if (obj == null)
            {
                return -1;
            }
            Job job=obj as Job;
            if (job == null)
            {
                throw new ArgumentException("Object is not Job");
            }
            return JobPriority.CompareTo(job.JobPriority);
        }
        // Type of job- used for logging
        protected abstract string JobType();
        // Every implementation returns job details - used for logging
        protected abstract string JobDetails();
        // Performs the job and does logging of time and job details
        internal SingleEntryTransferStatus DoRun()
        {
            Stopwatch timer=Stopwatch.StartNew();
            var result=DoJob();
            if (JobLog.IsDebugEnabled)
            {
                JobLog.Debug($"FileTransfer.Job, {JobType()}, {JobDetails()}, JobStatus: {result.Status}, Error: {result.Errors}, Lat: {timer.ElapsedMilliseconds}");
            }
            return result;

        }
        /// Performs the job and returns the transfer result of that job
        protected abstract SingleEntryTransferStatus DoJob();

        public override string ToString()
        {
            return JobPriority.ToString();
        }
    }
}
