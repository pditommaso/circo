package circo.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import akka.actor.ActorRef;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import groovy.lang.Closure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

public class AkkaHelper {

    static final Logger log = LoggerFactory.getLogger(AkkaHelper.class);

    public static Object ask( ActorRef target, Object message, long timeout )
            throws Exception
    {

        Duration duration = Duration.create(timeout, TimeUnit.MILLISECONDS);
        Future<Object> ask = Patterns.ask(target, message, duration.toMillis());
        return Await.result(ask, duration);
    }

    public static Future ask( ActorRef target, Object message, long  timeout, ExecutionContext context, final Closure closure )
            throws InterruptedException, TimeoutException, IllegalArgumentException
    {

        Duration duration = Duration.create(timeout, TimeUnit.MILLISECONDS);
        Future<Object> future = Patterns.ask(target, message, duration.toMillis());

        OnComplete<Object> callback = new OnComplete<Object>() {

            public void onComplete(Throwable failure, Object success) {
                log.debug( "Ask result: {}", success );
                closure.call(failure, success);
            }
        };

        future.onComplete( callback, context );
        return future;
    }



}