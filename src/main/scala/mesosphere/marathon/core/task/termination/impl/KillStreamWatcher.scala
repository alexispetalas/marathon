package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ ActorRef, Cancellable, PoisonPill }
import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.OverflowStrategy
import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.marathon.core.event.{ InstanceChanged, UnknownInstanceTerminated }
import mesosphere.marathon.core.instance.Instance

object KillStreamWatcher extends StrictLogging {

  private class CancellableActor(actorRef: ActorRef) extends Cancellable {
    private var _cancelled: Boolean = false

    def cancel(): Boolean = {
      actorRef ! PoisonPill
      _cancelled = true
      true
    }

    def isCancelled = _cancelled
  }

  // TODO - find better home
  private def eventBusSource[T](eventStream: akka.event.EventStream, bufferSize: Int = 32,
    overflowStrategy: OverflowStrategy = OverflowStrategy.fail)(implicit classTag: scala.reflect.ClassTag[T]): Source[T, Cancellable] = {
    val source = Source.actorRef[T](bufferSize, overflowStrategy)

    source.
      mapMaterializedValue { ref =>
        eventStream.subscribe(ref, classTag.runtimeClass)
        new CancellableActor(ref)
      }
  }

  private val killStreamWatcherId = new AtomicInteger
  def watchForKilledInstances(eventStream: akka.event.EventStream, instanceIds: Seq[Instance.Id]): Source[Done, Cancellable] = {
    import mesosphere.marathon.core.task.termination.InstanceChangedPredicates.considerTerminal

    val killedViaInstanceChanged =
      eventBusSource[InstanceChanged](eventStream).collect {
        case event if considerTerminal(event.condition) && instanceIds.contains(event.id) =>
          event.id
      }

    val killedViaUnknownInstanceTerminated =
      eventBusSource[UnknownInstanceTerminated](eventStream).collect {
        case UnknownInstanceTerminated(id, _, _) if instanceIds.contains(id) =>
          id
      }

    killedViaInstanceChanged.
      // eagerComplete closes the right in the case that left is cancelled
      merge(killedViaUnknownInstanceTerminated, eagerComplete = true).
      statefulMapConcat { () =>
        var pendingInstanceIds = instanceIds.toSet
        val name = s"kill-watcher-${killStreamWatcherId.getAndIncrement}"

        { (id: Instance.Id) =>
          pendingInstanceIds -= id
          logger.debug(s"Received terminal update for ${id}")
          if (pendingInstanceIds.isEmpty) {
            logger.info(s"${name} done watching; all watched instances were killed")
            List(Done)
          } else {
            logger.info(s"${name} still waiting for ${pendingInstanceIds.size} instances to be killed")
            Nil
          }
        }
      }
  }
}
