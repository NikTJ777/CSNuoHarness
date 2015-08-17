using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class ThreadPoolExecutor<T> where T : Runnable
    {
        internal readonly String name;
        internal readonly int maxThreads;
        internal BlockingCollection<ExecutorTask> queue;
        internal Semaphore semaphore;

        private static Logger log = Logger.getLogger("ThreadPoolExecutor");

        public ThreadPoolExecutor(String name, int maxThreads)
        {
            this.name = name;
            this.maxThreads = maxThreads;

            queue = new BlockingCollection<ExecutorTask>();
            semaphore = new Semaphore(maxThreads, maxThreads);

            for (int tx = 0; tx < maxThreads; tx++)
            {
                addThread();
            }
        }

        public void execute(T task)
        {
            queue.Add(new ExecutorTask(task, 0));
        }

        public void schedule(T task, int starttime)
        {
            queue.Add(new ExecutorTask(task, Environment.TickCount + starttime));
        }

        public long QueueSize()
        { return queue.Count; }

        public void shutdownNow()
        {
            queue.CompleteAdding();
        }

        public void awaitTermination(int timeout)
        {
            if (!queue.IsAddingCompleted)
                throw new Exception("ThreadPoolExecutor has not been shutdown - awaitTermination failed");

            // start the timeout timer...
            ThreadStart timer = new ThreadStart(() => { Thread.Sleep(timeout); semaphore.Release(maxThreads); });
            Thread timerThread = new Thread(timer);
            timerThread.Start();

            // wait for all threads to exit
            for (int tx = 0; tx < maxThreads; tx++)
            {
                semaphore.WaitOne();
            }

            timerThread.Abort();
        }

        protected void addThread()
        {
            ExecutorThread task = new ExecutorThread(this);
            ThreadStart threadDelegate = new ThreadStart(task.Run);
            Thread newThread = new Thread(threadDelegate);
            newThread.Name = String.Format("Thread-{0}-{1}", name, Environment.TickCount);
            newThread.Start();
        }

        internal class ExecutorTask
        {
            internal readonly T task;
            internal readonly int starttime;

            public ExecutorTask(T task, int starttime)
            {
                this.task = task;
                this.starttime = starttime;
            }
        }

        internal class ExecutorThread
        {
            internal ThreadPoolExecutor<T> executor;

            public ExecutorThread(ThreadPoolExecutor<T> executor)
            {
                this.executor = executor;
            }

            public void Run()
            {
                executor.semaphore.WaitOne();

                ExecutorTask task = null;
                ExecutorTask nextInLine = null;

                while (! executor.queue.IsAddingCompleted)
                {
                    //log.info("Looking for work...");

                    if (nextInLine != null) {
                        log.info("taking nextInLine...");
                        task = nextInLine;
                        nextInLine = null;
                    } else {
                        // block on next task
                        log.info("wait for next...");
                        try { task = executor.queue.Take(); }
                        catch (Exception e)
                        {
                            if (!executor.queue.IsAddingCompleted) {
                                log.info("Error while waiting for task {0}", e.ToString());
                                executor.addThread();
                            }
                            break;
                        }
                    }

                    if (task != null)
                    {
                        log.info("task starttime={0}", task.starttime);
                        if (task.starttime > 0)
                        {
                            // find sleeptime (if task starttime is still in the future)
                            int sleeptime = task.starttime - Environment.TickCount;
                            while (sleeptime > 0 && executor.queue.TryTake(out nextInLine))
                            {
                                if (nextInLine.starttime < task.starttime)
                                {
                                    ExecutorTask temp = task;
                                    task = nextInLine;
                                    nextInLine = temp;
                                    sleeptime = task.starttime - Environment.TickCount;
                                }
                                else
                                {
                                    break;
                                }
                            }

                            // sleep until starttime
                            if (sleeptime > 0)
                            {
                                log.info("Scheduled task. Sleeping for {0} ms", sleeptime);
                                Thread.Sleep(sleeptime);
                            }
                        }

                        //log.info("running task...");
                        try { task.task.run(); }
                        catch (Exception e)
                        {
                            log.info("Exception in ThreadPool thread: {0}\n{1}",
                                e.ToString(), e.StackTrace.ToString());

                            executor.semaphore.Release();
                            executor.addThread();

                            return;
                            //throw e;
                        }
                        //log.info("task complete.");
                    }

                }

                log.info("ThreadPoolExecutor is shutdown - thread exiting");
                executor.semaphore.Release();
                return;
            }
        }
    }
}
