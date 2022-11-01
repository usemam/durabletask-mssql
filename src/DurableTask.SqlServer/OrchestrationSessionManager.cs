namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Core;
    using Core.Common;
    using Core.History;
    using Microsoft.Data.SqlClient;

    class OrchestrationSessionManager
    {
        private readonly Dictionary<string, OrchestrationSession> activeOrchestrationSessions =
            new Dictionary<string, OrchestrationSession>(StringComparer.OrdinalIgnoreCase);
        private readonly LogHelper traceHelper;
        private readonly BackoffPollingHelper orchestrationBackoffHelper;
        private readonly SqlOrchestrationServiceSettings settings;
        private readonly string lockedByValue;
        private readonly object sessionLock = new object();

        public OrchestrationSessionManager(
            SqlOrchestrationServiceSettings settings,
            LogHelper traceHelper,
            BackoffPollingHelper orchestrationBackoffHelper)
        {
            this.traceHelper = traceHelper;
            this.orchestrationBackoffHelper = orchestrationBackoffHelper;
            this.settings = settings;
            this.lockedByValue = $"{this.settings.AppName},{Process.GetCurrentProcess().Id}";
        }

        public async Task<OrchestrationSession?> GetNextSessionAsync(DateTime lockExpiration, CancellationToken cancellationToken)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
            using SqlCommand command = GetSprocCommand(connection, $"{this.settings.SchemaName}._LockNextOrchestration");
            
            int batchSize = this.settings.WorkItemBatchSize;

            command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
            command.Parameters.Add("@LockedBy", SqlDbType.VarChar, 100).Value = this.lockedByValue;
            command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;
            
            DbDataReader reader;

            try
            {
                reader = await SqlUtils.ExecuteReaderAsync(
                    command,
                    this.traceHelper,
                    instanceId: null,
                    cancellationToken);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, new OrchestrationInstance());
                throw;
            }
            
            using (reader)
            {
                // Result #1: The list of control queue messages
                int longestWaitTime = 0;
                var messages = new List<TaskMessage>(capacity: batchSize);
                var eventPayloadMappings = new EventPayloadMap(capacity: batchSize);
                while (await reader.ReadAsync(cancellationToken))
                {
                    TaskMessage message = reader.GetTaskMessage();
                    messages.Add(message);
                    Guid? payloadId = reader.GetPayloadId();
                    if (payloadId.HasValue)
                    {
                        // TODO: Need to understand what the payload behavior is for retry events
                        eventPayloadMappings.Add(message.Event, payloadId.Value);
                    }

                    // TODO: We're not currently using this value for anything. Ideally it would be included
                    //       in some logging that still needs to be introduced.
                    longestWaitTime = Math.Max(longestWaitTime, reader.GetInt32("WaitTime"));
                }

                if (messages.Count == 0)
                {
                    // TODO: Make this dynamic based on the number of readers
                    await this.orchestrationBackoffHelper.WaitAsync(cancellationToken);
                    return null;
                }

                this.orchestrationBackoffHelper.Reset();

                // Result #2: The full event history for the locked instance
                IList<HistoryEvent> history;
                if (await reader.NextResultAsync(cancellationToken))
                {
                    history = await ReadHistoryEventsAsync(reader, executionIdFilter: null, cancellationToken);
                }
                else
                {
                    this.traceHelper.GenericWarning(
                        details: "Failed to read history from the database!",
                        instanceId: messages.FirstOrDefault(m => m.OrchestrationInstance?.InstanceId != null)?.OrchestrationInstance.InstanceId);
                    history = Array.Empty<HistoryEvent>();
                }

                var runtimeState = new OrchestrationRuntimeState(history);

                OrchestrationInstance instance;
                if (runtimeState.ExecutionStartedEvent != null)
                {
                    // This is an existing instance
                    instance = runtimeState.OrchestrationInstance!;
                }
                else if (messages[0].Event is ExecutionStartedEvent startedEvent)
                {
                    // This is a new manually-created instance
                    instance = startedEvent.OrchestrationInstance;
                }
                else if (Entities.AutoStart(messages[0].OrchestrationInstance.InstanceId, messages) &&
                         messages[0].Event is ExecutionStartedEvent autoStartedEvent)
                {
                    // This is a new auto-start instance (e.g. Durable Entities)
                    instance = autoStartedEvent.OrchestrationInstance;
                }
                else
                {
                    // Don't know what to do with this message (TODO: Need to confirm behavior)
                    instance = new OrchestrationInstance();
                }

                lock (this.sessionLock)
                {
                    var session = new OrchestrationSession(instance, runtimeState, messages, eventPayloadMappings);
                    this.activeOrchestrationSessions.Add(instance.InstanceId, session);

                    return session;
                }
            }
        }
        
        public bool TryGetExistingSession(string instanceId, out OrchestrationSession session)
        {
            lock (this.sessionLock)
            {
                return this.activeOrchestrationSessions.TryGetValue(instanceId, out session);
            }
        }
        
        async Task<SqlConnection> GetAndOpenConnectionAsync(CancellationToken cancellationToken)
        {
            SqlConnection connection = this.settings.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            return connection;
        }

        static SqlCommand GetSprocCommand(SqlConnection connection, string sprocName)
        {
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = sprocName;
            return command;
        }
        
        static async Task<List<HistoryEvent>> ReadHistoryEventsAsync(
            DbDataReader reader,
            string? executionIdFilter = null,
            CancellationToken cancellationToken = default)
        {
            var history = new List<HistoryEvent>(capacity: 128);
            while (await reader.ReadAsync(cancellationToken))
            {
                string executionId = SqlUtils.GetExecutionId(reader)!;
                HistoryEvent e = reader.GetHistoryEvent(isOrchestrationHistory: true);
                if (executionIdFilter == null)
                {
                    executionIdFilter = executionId;
                }
                else if (executionIdFilter != executionId)
                {
                    // Either the instance with this execution ID doesn't exist or
                    // we're reading past the end of the current history (ContinueAsNew case).
                    break;
                }

                history.Add(e);
            }

            return history;
        }
    }
}