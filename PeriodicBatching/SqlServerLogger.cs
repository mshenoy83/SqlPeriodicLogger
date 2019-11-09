using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using FastMember;
using Polly;
using Polly.Retry;

namespace PeriodicDatabaseLogger.PeriodicBatching
{

    public class SqlServerPeriodicLogger<T> : IDisposable where T : class, IMappingDetails
    {
        readonly RetryPolicy _retryPolicy;
        readonly string _connectionString;
        readonly string _schemaName;
        readonly int _batchSizeLimit;
        readonly ConcurrentQueue<T> _queue;
        readonly BatchedConnectionStatus _status;
        readonly Queue<T> _waitingBatch = new Queue<T>();

        readonly object _stateLock = new object();

        readonly PortableTimer _timer;

        bool _unloading;
        bool _started;


        /// <summary> 
        /// </summary>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="connectionString">Connection string to access the database.</param>
        /// <param name="schemaName">Name of the schema for the table to store the data in. The default is 'dbo'.</param>
        /// <param name="batchSizeLimit">The maximum number of events to include in a single batch. Defaulted to 50</param>
        /// <param name="retryCount">The maximum number of times the logger will try to retry the insert into the database in case of failure. Defaulted to 3</param>
        public SqlServerPeriodicLogger(TimeSpan period, string connectionString, string schemaName = "dbo", int batchSizeLimit = 50, int retryCount = 3)
        {
            _connectionString = connectionString;
            _schemaName = schemaName;
            if (batchSizeLimit <= 0)
                throw new ArgumentOutOfRangeException(nameof(batchSizeLimit), "The batch size limit must be greater than zero.");
            if (period <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(period), "The period must be greater than zero.");

            _batchSizeLimit = batchSizeLimit;
            _queue = new ConcurrentQueue<T>();
            _status = new BatchedConnectionStatus(period);

            _timer = new PortableTimer(cancel => OnTick());

            _retryPolicy = Policy
                           .Handle<Exception>()
                           .WaitAndRetryAsync(retryCount, i => TimeSpan.FromSeconds(1));
        }

        void CloseAndFlush()
        {
            lock (_stateLock)
            {
                if (!_started || _unloading)
                    return;

                _unloading = true;
            }

            _timer.Dispose();

            // This is the place where SynchronizationContext.Current is unknown and can be != null
            // so we prevent possible deadlocks here for sync-over-async downstream implementations 
            ResetSyncContextAndWait(OnTick);
        }

        void ResetSyncContextAndWait(Func<Task> taskFactory)
        {
            var prevContext = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            try
            {
                taskFactory().Wait();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prevContext);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Free resources held by the sink.
        /// </summary>
        /// <param name="disposing">If true, called because the object is being disposed; if false,
        /// the object is being disposed from the finalizer.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            CloseAndFlush();
        }

        private async Task EmitBatchAsync(IEnumerable<T> events)
        {
            List<T> eventList = events.ToList();
            if (!eventList.Any())
            {
                return;
            }
            T randomevent = eventList.FirstOrDefault();
            try
            {
                using (var cn = new SqlConnection(_connectionString))
                using (var datareader = ObjectReader.Create(eventList))
                {
                    await cn.OpenAsync().ConfigureAwait(false);
                    using (var copy = new SqlBulkCopy(cn, SqlBulkCopyOptions.Default, null))
                    {
                        // ReSharper disable once PossibleNullReferenceException
                        copy.DestinationTableName = $"[{_schemaName}].[{randomevent.TableName()}]";
                        foreach (SqlBulkCopyColumnMapping mapping in randomevent.ColumnMappings()
                                                                                .Select(column => new SqlBulkCopyColumnMapping(column, column)))
                        {
                            copy.ColumnMappings.Add(mapping);
                        }

                        copy.EnableStreaming = true;
                        await copy.WriteToServerAsync(datareader).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
                throw;
            }
        }



        async Task OnTick()
        {
            try
            {
                bool batchWasFull;
                do
                {
                    while (_waitingBatch.Count < _batchSizeLimit &&
                        _queue.TryDequeue(out T next))
                    {
                        _waitingBatch.Enqueue(next);
                    }

                    await _retryPolicy.ExecuteAsync(async () => await EmitBatchAsync(_waitingBatch));
                    

                    batchWasFull = _waitingBatch.Count >= _batchSizeLimit;
                    _waitingBatch.Clear();
                    _status.MarkSuccess();
                }
                while (batchWasFull); // Otherwise, allow the period to elapse
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Exception while emitting periodic batch from {0}: {1}", this, ex);
                _status.MarkFailure();
            }
            finally
            {
                if (_status.ShouldDropBatch)
                    _waitingBatch.Clear();

                if (_status.ShouldDropQueue)
                {
                    while (_queue.TryDequeue(out _)) { }
                }

                lock (_stateLock)
                {
                    if (!_unloading)
                        SetTimer(_status.NextInterval);
                }
            }
        }

        void SetTimer(TimeSpan interval)
        {
            _timer.Start(interval);
        }


        public void AddLog(T logEvent)
        {
            if (logEvent == null) throw new ArgumentNullException(nameof(logEvent));

            if (_unloading)
                return;

            if (!_started)
            {
                lock (_stateLock)
                {
                    if (_unloading) return;
                    if (!_started)
                    {
                        // Special handling to try to get the first event across as quickly
                        // as possible to show we're alive!
                        _queue.Enqueue(logEvent);
                        _started = true;
                        SetTimer(TimeSpan.Zero);
                        return;
                    }
                }
            }

            _queue.Enqueue(logEvent);
        }
    }
}