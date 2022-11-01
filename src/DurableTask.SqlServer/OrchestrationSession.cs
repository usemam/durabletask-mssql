namespace DurableTask.SqlServer
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Core;

    sealed class OrchestrationSession : IOrchestrationSession
    {
        public OrchestrationInstance Instance { get; }

        public EventPayloadMap EventPayloadMappings { get; }
        
        public OrchestrationRuntimeState RuntimeState { get; }
        
        public List<TaskMessage> Messages { get; }

        public OrchestrationSession(
            OrchestrationInstance instance,
            OrchestrationRuntimeState runtimeState,
            List<TaskMessage> messages,
            EventPayloadMap eventPayloadMappings)
        {
            this.Instance = instance;
            this.RuntimeState = runtimeState;
            this.Messages = messages;
            this.EventPayloadMappings = eventPayloadMappings;
        }
        
        public Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new System.NotImplementedException();
        }
    }
}