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
        internal readonly int maxThreads;
        internal BlockingCollection<ExecutorTask> queue;
        internal Semaphore semaphore;

        private static Logger log = Logger.getLogger("ThreadPoolExecutor");

        public ThreadPoolExecutor(int maxThreads)
        {
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
            queue.Add(new ExecutorTask(task, starttime));
        }

        public long QueueSize()
        { return queue.Count; }

        public void shutdownNow()
        {
            queue.CompleteAdding();
        }

        public void awaitTermination()
        {
            if (!queue.IsAddingCompleted)
                throw new Exception("ThreadPoolExecutor has not been shutdown - awaitTermination failed");

            // wait for all threads to exit
            for (int tx = 0; tx < maxThreads; tx++)
            {
                semaphore.WaitOne();
            }
        }

        protected void addThread()
        {
            ExecutorThread task = new ExecutorThread(this);
            ThreadStart threadDelegate = new ThreadStart(task.Run);
            Thread newThread = new Thread(threadDelegate);
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

                do
                {
                    if (nextInLine != null) {
                        task = nextInLine;
                        nextInLine = null;
                    } else {
                        // block on next task
                        task = executor.queue.Take();
                    }

                    if (task != null)
                    {
                        // find sleeptime (if task starttime is still in the future)
                        int sleeptime = task.starttime - Environment.TickCount;
                        while (sleeptime > 0 && executor.queue.TryTake(out nextInLine)) {
                            if (nextInLine.starttime < task.starttime)
                            {
                                ExecutorTask temp = task;
                                task = nextInLine;
                                nextInLine = temp;
                                sleeptime = task.starttime - Environment.TickCount;
                            } else {
                                break;
                            }
                        }

                        // sleep until starttime
                        if (sleeptime > 0)
                        {
                            Thread.Sleep(sleeptime);
                        }

                        try { task.task.run(); }
                        catch (Exception e)
                        {
                            log.info("Exception in ThreadPool thread: {0}", e.ToString());
                            executor.semaphore.Release();
                            executor.addThread();

                            throw e;
                        }
                    }

                } while (! executor.queue.IsAddingCompleted);

                log.info("ThreadPoolExecutor is shutdown - thread exiting");
                executor.semaphore.Release();
                return;
            }
        }
    }
}
