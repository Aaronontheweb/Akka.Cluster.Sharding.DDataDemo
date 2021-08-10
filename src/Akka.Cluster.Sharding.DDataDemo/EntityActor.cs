using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;

namespace Petabridge.App
{
    /// <summary>
    /// Actor that just logs what it receives
    /// </summary> 
    public class EntityActor : ReceiveActor
    {
        private int _count = 0;
        private readonly string _entityId;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public EntityActor(string entityId)
        {
            _entityId = entityId;
            
            ReceiveAny(o =>
            {
                _log.Info("Received [{0}] - {1} messages received so far", o, ++_count);
            });
        }
    }

    public interface IWithEntityId
    {
        string EntityId { get; }
    }

    public sealed class EntityCmd : IWithEntityId
    {
        public EntityCmd(string entityId)
        {
            EntityId = entityId;
        }

        public string EntityId { get; }

        public override string ToString()
        {
            return $"{GetType()}(EntityId={EntityId})";
        }
    }

    public sealed class EntityRouter : HashCodeMessageExtractor
    {
        public EntityRouter(int maxNumberOfShards) : base(maxNumberOfShards)
        {
        }

        public override string EntityId(object message)
        {
            switch (message)
            {
                case IWithEntityId e:
                    return e.EntityId;
                case ShardRegion.StartEntity s:
                    {
                        return s.EntityId;
                    }
                default:
                    return null;
            }
        }
    }
}