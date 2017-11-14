using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store
{
    internal abstract class BaseJob : IComparable
    {
        /// <summary>
        /// Priority- Represents it's position in the max-heap. Higher priority means the job will be completed quicker.
        /// ChunkedFileCopy has higher priority than NonChunkedFile copy and concatenate job
        /// </summary>
        protected long JobPriority { get; }

        protected BaseJob(long priority)
        {
            JobPriority = priority;
        }
        public int CompareTo(object obj)
        {
            if (obj == null)
            {
                return -1;
            }
            var job = obj as BaseJob;
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

        /// Performs the job and returns the transfer result of that job
        protected abstract object DoJob();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="jobLog"></param>
        /// <returns></returns>
        internal object DoRun(Logger jobLog=null)
        {
            bool isLogging = jobLog != null && jobLog.IsDebugEnabled;
            Stopwatch timer = isLogging?Stopwatch.StartNew():null;
            var res=DoJob();
            if (isLogging)
            {
                var result = res != null ? $", {res.ToString()}" :string.Empty;
                jobLog.Debug($"{JobType()}, {JobDetails()}, Lat: {timer.ElapsedMilliseconds}{result}");
            }
            return res;
        }
        public override string ToString()
        {
            return JobPriority.ToString();
        }
    }
}
