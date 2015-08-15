using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace NuoTest
{
    class Controller : IDisposable
    {
    internal OwnerRepository ownerRepository;
    internal EventRepository eventRepository;
    internal GroupRepository groupRepository;
    internal DataRepository dataRepository;

    //internal ExecutorService insertExecutor;
    //internal ScheduledExecutorService queryExecutor;

    internal ThreadPoolExecutor<EventGenerator> insertExecutor;
    internal ThreadPoolExecutor<EventViewTask> queryExecutor;

    Dictionary<String, String> appProperties;

    internal long runTime;
    internal float averageRate, timingSpeedup;
    internal int minViewAfterInsert, maxViewAfterInsert;
    internal int minGroups, maxGroups;
    internal int minData, maxData;
    internal float burstProbability;
    internal int minBurst, maxBurst;
    internal int maxQueued, queryBackoff;
    bool initDb = false;
    bool queryOnly = false;

    internal TxModel txModel;
    internal SqlSession.Mode bulkCommitMode;

    internal Int64 totalScheduled = 0;
    internal Int64 totalInserts = 0;
    internal Int64 totalInsertTime = 0;

    internal Int64 totalQueries = 0;
    internal Int64 totalQueryRecords = 0;
    internal Int64 totalQueryTime = 0;

    long unique;

    long totalEvents;
    long wallTime;

    private Random random = new Random();

    private Dictionary<String, String> defaultProperties = new Dictionary<String, String>();

    public const String PROPERTIES_PATH =    "properties.path";
    public const String AVERAGE_RATE =       "timing.rate";
    public const String MIN_VIEW_DELAY =     "timing.min.view.delay";
    public const String MAX_VIEW_DELAY =     "timing.max.view.delay";
    public const String TIMING_SPEEDUP =     "timing.speedup";
    public const String INSERT_THREADS =     "insert.threads";
    public const String QUERY_THREADS =      "query.threads";
    public const String MAX_QUEUED =         "max.queued";
    public const String DB_PROPERTIES_PATH = "db.properties.path";
    public const String RUN_TIME =           "run.time";
    public const String MIN_GROUPS =         "min.groups";
    public const String MAX_GROUPS =         "max.groups";
    public const String MIN_DATA =           "min.data";
    public const String MAX_DATA =           "max.data";
    public const String BURST_PROBABILITY_PERCENT = "burst.probability.percent";
    public const String MIN_BURST =          "min.burst";
    public const String MAX_BURST =          "max.burst";
    public const String DB_INIT =            "db.init";
    public const String DB_INIT_SQL =        "db.init.sql";
    public const String DB_SCHEMA =          "db.schema";
    public const String TX_MODEL =           "tx.model";
    public const String BULK_COMMIT_MODE =   "bulk.commit.mode";
    public const String QUERY_ONLY =         "query.only";
    public const String QUERY_BACKOFF =      "query.backoff";
    public const String UPDATE_ISOLATION =   "update.isolation";
    public const String CONNECTION_TIMEOUT = "connection.timeout";
    public const String DB_PROPERTY_PREFIX = "db.property.prefix";

    internal enum TxModel { DISCRETE, UNIFIED };

    internal static Logger appLog = Logger.getLogger("CSNuoTest");
    internal static Logger insertLog = Logger.getLogger("InsertLog");
    internal static Logger viewLog = Logger.getLogger("EventViewer");

    internal const double Nano2Millis = 1000000.0;
    internal const double Nano2Seconds = 1000000000.0;
    internal const double Millis2Seconds = 1000.0;

    internal const int Millis = 1000;

    internal const float Percent = 100.0f;

    public Controller() {
        defaultProperties.Add(PROPERTIES_PATH, "classpath://NuoTest.Application.properties");
        defaultProperties.Add(DB_PROPERTIES_PATH, "classpath://NuoTest.Database.properties");
        defaultProperties.Add(AVERAGE_RATE, "0");
        defaultProperties.Add(MIN_VIEW_DELAY, "0");
        defaultProperties.Add(MAX_VIEW_DELAY, "0");
        defaultProperties.Add(TIMING_SPEEDUP, "1");
        defaultProperties.Add(INSERT_THREADS, "1");
        defaultProperties.Add(QUERY_THREADS, "1");
        defaultProperties.Add(MAX_QUEUED, "0");
        defaultProperties.Add(MIN_GROUPS, "1");
        defaultProperties.Add(MAX_GROUPS, "5");
        defaultProperties.Add(MIN_DATA, "500");
        defaultProperties.Add(MAX_DATA, "3500");
        defaultProperties.Add(BURST_PROBABILITY_PERCENT, "0");
        defaultProperties.Add(MIN_BURST, "0");
        defaultProperties.Add(MAX_BURST, "0");
        defaultProperties.Add(RUN_TIME, "5");
        defaultProperties.Add(TX_MODEL, "DISCRETE");
        defaultProperties.Add(BULK_COMMIT_MODE, "BATCH");
        defaultProperties.Add(DB_INIT, "false");
        defaultProperties.Add(QUERY_ONLY, "false");
        defaultProperties.Add(QUERY_BACKOFF, "0");
        defaultProperties.Add(UPDATE_ISOLATION, "CONSISTENT_READ");
        defaultProperties.Add(CONNECTION_TIMEOUT, "300");
    }

    public void configure(String[] args)
    {
        // create app properties, using the default values as initial values
        appProperties = new Dictionary<String, String>(defaultProperties);

        // parse command line on top of defaults
        parseCommandLine(args, appProperties);

        // load properties from application.properties file over the defaults
        loadProperties(appProperties, PROPERTIES_PATH);

        // now load database properties file over the application and the defaults
        loadProperties(appProperties, DB_PROPERTIES_PATH);

        // parse the command line into app properties, as command line overrides all others
        parseCommandLine(args, appProperties);

        String[] keys = appProperties.Keys.ToArray();
        Array.Sort(keys);
            
        String helpOption;
        if (appProperties.TryGetValue("help", out helpOption) && "true".Equals(helpOption, StringComparison.InvariantCultureIgnoreCase)) {
            Console.Out.WriteLine("\nCSNuoTest [option=value [, option=value, ...] ]\nwhere <option> can be any of:\n");

            foreach (String key in keys) {
                Console.Out.WriteLine(String.Format("{0}\t\t\t\t(default={1})", key, defaultProperties[key]));
            }

            Console.Out.WriteLine("\nHelp called - nothing to do; exiting.");
            Environment.Exit(0);
        }

        appLog.info("command-line properties: {0}",  string.Join(";", appProperties));

        StringBuilder builder = new StringBuilder(1024);
        builder.Append("\n***************** Resolved Properties ********************\n");
        foreach (String key in keys) {
            builder.AppendFormat("{0} = {1}\n", key, appProperties[key]);
        }
        appLog.info("{0}**********************************************************\n", builder.ToString());

        runTime = Int32.Parse(appProperties[RUN_TIME]) * Millis;
        averageRate = Single.Parse(appProperties[AVERAGE_RATE]);
        minViewAfterInsert = Int32.Parse(appProperties[MIN_VIEW_DELAY]);
        maxViewAfterInsert = Int32.Parse(appProperties[MAX_VIEW_DELAY]);
        timingSpeedup = Single.Parse(appProperties[TIMING_SPEEDUP]);
        minGroups = Int32.Parse(appProperties[MIN_GROUPS]);
        maxGroups = Int32.Parse(appProperties[MAX_GROUPS]);
        minData = Int32.Parse(appProperties[MIN_DATA]);
        maxData = Int32.Parse(appProperties[MAX_DATA]);
        burstProbability = Single.Parse(appProperties[BURST_PROBABILITY_PERCENT]);
        minBurst = Int32.Parse(appProperties[MIN_BURST]);
        maxBurst = Int32.Parse(appProperties[MAX_BURST]);
        maxQueued = Int32.Parse(appProperties[MAX_QUEUED]);
        initDb = Boolean.Parse(appProperties[DB_INIT]);
        queryOnly = Boolean.Parse(appProperties[QUERY_ONLY]);
        queryBackoff = Int32.Parse(appProperties[QUERY_BACKOFF]);

        String threadParam;
        int insertThreads = (appProperties.TryGetValue(INSERT_THREADS, out threadParam) ? Int32.Parse(threadParam) : 1);

        int queryThreads = (appProperties.TryGetValue(QUERY_THREADS, out threadParam) ? Int32.Parse(threadParam) : 1);

        if (maxViewAfterInsert > 0 && maxViewAfterInsert < minViewAfterInsert) {
            maxViewAfterInsert = minViewAfterInsert;
        }

        if (maxBurst <= minBurst) {
            appLog.info("maxBurst ({0}) <= minBurst ({1}); burst disabled", maxBurst, minBurst);
            burstProbability = minBurst = maxBurst = 0;
        }

        // filter out database properties, and strip off the prefix
        Dictionary<String, String> dbProperties = new Dictionary<String, String>();
        String dbPropertyPrefix = appProperties[DB_PROPERTY_PREFIX];
        if (! dbPropertyPrefix.EndsWith(".")) dbPropertyPrefix = dbPropertyPrefix + ".";

        foreach (String key in appProperties.Keys) {
            if (key.StartsWith(dbPropertyPrefix)) {
                dbProperties[key.Substring(dbPropertyPrefix.Length)] = appProperties[key];
            }
        }

        //String insertIsolation = appProperties.getProperty(UPDATE_ISOLATION);
        //DataSource dataSource = new com.nuodb.jdbc.DataSource(dbProperties);
        SqlSession.init(dbProperties, insertThreads + queryThreads);

        ownerRepository = new OwnerRepository();
        ownerRepository.init();

        groupRepository = new GroupRepository();
        groupRepository.init();

        dataRepository = new DataRepository();
        dataRepository.init();

        eventRepository = new EventRepository(ownerRepository, groupRepository, dataRepository);
        eventRepository.init();

        if (!Enum.TryParse<TxModel>(appProperties[TX_MODEL], out txModel))
            txModel = TxModel.DISCRETE;

        if (!Enum.TryParse<SqlSession.Mode>(appProperties[BULK_COMMIT_MODE], out bulkCommitMode))
            bulkCommitMode = SqlSession.Mode.BATCH;

        //insertExecutor = Executors.newFixedThreadPool(insertThreads);
        //queryExecutor= Executors.newScheduledThreadPool(queryThreads);

        insertExecutor = new ThreadPoolExecutor<EventGenerator>("INSERT", insertThreads);
        queryExecutor = new ThreadPoolExecutor<EventViewTask>("QUERY", queryThreads);

        string checkOnly;
        if (appProperties.TryGetValue("check.config", out checkOnly) && checkOnly.Equals("true", StringComparison.InvariantCultureIgnoreCase)) {
            Console.Out.WriteLine("CheckConfig called - nothing to do; exiting.");
            Environment.Exit(0);
        }
    }

    /**
     * perform any logic required after configuration, and before the Controller can be used
     */
    public void init() {
        if (initDb) {
            initializeDatabase();
            unique = 1;
        } else {
            using (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                String lastEventId = eventRepository.getValue("id", "ORDER BY id DESC LIMIT 1");
                unique = Int64.Parse(lastEventId) + 1;
                appLog.info("lastEventID = {0}", lastEventId);
            }
        }

    }

    /**
     * Start the controller.
     *
     * @throws InterruptedException
     */
    public void run()
    {
        long start = Environment.TickCount;
        long endTime = start + runTime;
        long now;

        double currentRate = 0.0;
        long averageSleep = (long) (Millis2Seconds / averageRate);

        totalEvents = 0;
        wallTime = 0;

        double burstRate = 0.0;
        int burstSize = 0;

        // ensure that first sample time is different from start time...
        int settleTime = 2 * Millis;
        appLog.info("Settling for {0}: ", settleTime);
        Thread.Sleep(settleTime);

        // just run some queries
        if (queryOnly) {

            long eventId = 1;

            while (Environment.TickCount < endTime) {
                EventViewTask viewTask = new EventViewTask(this, eventId++);
                //queryExecutor.schedule(new EventViewTask(this, eventId++), 2, TimeUnit.MILLISECONDS);
                queryExecutor.schedule(viewTask, 2);

                totalEvents++;

                appLog.info("Processed {0:N} events containing {1:N} records in {2:F2} secs"
                                + "\n\tThroughput:\t{3:F2} events/sec at {4:F2} ips;"
                                + "\n\tSpeed:\t\t{5:N} inserts in {6:F2} secs = {7:F2} ips"
                                + "\n\tQueries:\t{8:N} queries got {9:N} records in {10:F2} secs at {11:F2} qps",
                        totalEvents, totalInserts, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts / wallTime),
                        totalInserts, (totalInsertTime / Nano2Seconds), (Nano2Seconds * totalInserts / totalInsertTime),
                        totalQueries, totalQueryRecords, (totalQueryTime / Nano2Seconds), (Nano2Seconds * totalQueries / totalQueryTime));

                //if (((ThreadPoolExecutor) queryExecutor).getQueue().size() > 10) {
                if (totalEvents + 10 > totalQueries) {
                    appLog.info("{0} queries waiting - sleeping", totalEvents - totalQueries);
                    Thread.Sleep(200);
                }
            }

            return;
        }


        do {
            EventGenerator generator = new EventGenerator(this, unique++);
            //insertExecutor.execute(new EventGenerator(this, unique++));
            insertExecutor.execute(generator);

            totalEvents++;

            //int queueSize = ((ThreadPoolExecutor) insertExecutor).getQueue().size());
            //Int64 queueSize = totalScheduled - totalEvents;
            long queueSize = insertExecutor.QueueSize();
            appLog.info("Event scheduled. Queue size={0}", queueSize);

            now = Environment.TickCount;
            currentRate = (Millis2Seconds * totalEvents) / (now - start);

            appLog.info("now={0}; endTime={1}; elapsed={2}; time left={3}", now, endTime, now - start, endTime - now);

            // randomly create a burst
            if (burstSize == 0 && burstProbability > 0 && Percent * random.NextDouble() <= burstProbability) {
                burstSize = minBurst + random.Next(maxBurst - minBurst);
                appLog.info("Creating burst of {0}", burstSize);
            }

            if (burstSize > 0) {
                burstSize--;
            } else {
                if (averageRate > 0) {
                    int sleepTime = (int) (averageSleep * (currentRate / averageRate));
                    if (now + sleepTime > endTime) sleepTime = 1 * Millis;

                    appLog.info("Current Rate= {0:F2}; sleeping for {1:N} ms", currentRate, sleepTime);

                    if (timingSpeedup > 1) {
                        sleepTime = (int)(sleepTime / timingSpeedup);
                        appLog.info("Warp-drive: speedup {0:F}; sleeping for {1} ms", timingSpeedup, sleepTime);
                    }

                    Thread.Sleep(sleepTime);
                }

                //queueSize = ((ThreadPoolExecutor) insertExecutor).getQueue().size();
                //queueSize = totalScheduled = totalInserts;
                while (maxQueued >= 0 && insertExecutor.QueueSize() > maxQueued) {
                    //queueSize = totalScheduled = totalInserts;
                    queueSize = insertExecutor.QueueSize();
                    appLog.info("Queue size {0} is over limit {1} - sleeping", queueSize, maxQueued);
                    Thread.Sleep(1 * Millis / (queueSize > 1 ? 2 : 20));

                    if (insertExecutor.QueueSize() > maxQueued)
                    {
                        appLog.info("Queue still has {0} items; there are {1} active SqlSessions",
                            insertExecutor.QueueSize(), SqlSession.activeSessions());
                    }
                }

                // queueSize = ((ThreadPoolExecutor) insertExecutor).getQueue().size();
                //queueSize = totalScheduled - totalInserts;
                queueSize = insertExecutor.QueueSize();
                appLog.info("Sleeping done. Queue size={0}", queueSize);

            }

            wallTime = Environment.TickCount - start;

            appLog.info("Processed {0:N0} events containing {1:N0} records in {2:F2} secs"
                            + "\n\tThroughput:\t{3:F2} events/sec at {4:F2} ips;"
                            + "\n\tSpeed:\t\t{5:N0} inserts in {6:F2} secs = {7:F2} ips"
                            + "\n\tQueries:\t{8:N0} queries got {9:N} records in {10:F2} secs at {11:F2} qps",
                    totalEvents, totalInserts, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts / wallTime),
                    totalInserts, (totalInsertTime / Millis2Seconds), (Millis2Seconds * totalInserts / totalInsertTime),
                    totalQueries, totalQueryRecords, (totalQueryTime / Millis2Seconds), (Millis2Seconds * totalQueries / totalQueryTime));


        } while (Environment.TickCount < endTime);
    }

    public void Dispose()
    {
        if (insertExecutor != null)
        {
            insertExecutor.shutdownNow();
            //insertExecutor.awaitTermination(10, TimeUnit.SECONDS);
            insertExecutor.awaitTermination(10000);
        }
        if (queryExecutor != null)
        {
            queryExecutor.shutdownNow();
            //queryExecutor.awaitTermination(10, TimeUnit.SECONDS);
            queryExecutor.awaitTermination(10000);
        }
        // queueSize = ((ThreadPoolExecutor) insertExecutor).getQueue().size();
        //Int64 queueSize = totalScheduled - totalInserts;
        long queueSize = insertExecutor.QueueSize();

        appLog.info("Exiting with {0} items remaining in the queue.\n\tProcessed {1:N0} events containing {2:N0} records in {3:F2} secs"
                        + "\n\tThroughput:\t{4:F2} events/sec at {5:F2} ips;"
                        + "\n\tSpeed:\t\t{6:N0} inserts in {7:F2} secs = {8:F2} ips"
                        + "\n\tQueries:\t{9:N0} queries got {10:N} records in {11:F2} secs at {12:F2} qps",
                queueSize,
                totalEvents, totalInserts, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts / wallTime),
                totalInserts, (totalInsertTime / Millis2Seconds), (Millis2Seconds * totalInserts / totalInsertTime),
                totalQueries, totalQueryRecords, (totalQueryTime / Millis2Seconds), (Millis2Seconds * totalQueries / totalQueryTime));

        //appLog.info(String.format("Exiting with %d items remaining in the queue.\n\tProcessed %,d events containing %,d records in %.2f secs\n\tThroughput:\t%.2f events/sec at %.2f ips;\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips",
        //        ((ThreadPoolExecutor) insertExecutor).getQueue().size(),
        //        totalEvents, totalInserts/*.get()*/, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts/*.get()*/ / wallTime),
        //        totalInserts/*.get()*/, (totalInsertTime/*.get()*/ / Nano2Seconds), (Nano2Seconds * totalInserts/*.get()*/ / totalInsertTime/*.get()*/)));

        SqlSession.cleanup();
    }

    protected void initializeDatabase() {
        String script;
        if (!appProperties.TryGetValue(DB_INIT_SQL, out script))
            appLog.log("Somehow script is NULL");

        appLog.info("running init sql (length: {0}): {1}", script.Length, script);
        using (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
            session.execute(script);
        }
    }

    protected void parseCommandLine(String[] args, Dictionary<String, String> props) {

        foreach (String param in args) {
            String[] keyVal = param.Split(new char[] {'='});
            if (keyVal.Length == 2) {
                props[keyVal[0].Trim().Replace("-", "")] = keyVal[1];
            }
            else {
                props[param.Trim().Replace("-", "")] = "true";
            }
        }
    }

    protected void loadProperties(Dictionary<String, String> props, String key)
    {
        if (key == null || key.Length == 0)
            throw new ArgumentException("Null or empty key", "key");

        String path;
        if (!appProperties.TryGetValue(key, out path) || path.Length == 0) {
            appLog.info("loadProperties: key {0} not in app properties", key);
            return;
        }

        appLog.info("loading properties: {0} from {1}", key, path);
        Dictionary<String, String> localKeys = new Dictionary<string, string>();
        Stream stream;
        if (path.StartsWith("classpath://")) {
            stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(path.Substring("classpath://".Length));
            appLog.info("loading resource: {0}", path.Substring("classpath://".Length));
        } else {
            stream = new FileStream(path, FileMode.Open);
        }
        if (stream != null)
        {
            try
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if ((!String.IsNullOrEmpty(line)) &&
                            (!line.StartsWith(";")) &&
                            (!line.StartsWith("#")) &&
                            (!line.StartsWith("'")) &&
                            (line.Contains('=')))
                        {
                            // if the line ends with a \, concatenate it with the next line
                            while (line.EndsWith("\\"))
                            {
                                string nextLine = reader.ReadLine();
                                if (nextLine == null)
                                    break;
                                line = line.TrimEnd(new char[] { '\\' }) + nextLine;
                            }
                            // convert explicit \n
                            line = line.Replace("\\n", "\n");
                            int index = line.IndexOf('=');
                            String k = line.Substring(0, index).Trim();
                            String v = line.Substring(index + 1).Trim();

                            if ((v.StartsWith("\"") && v.EndsWith("\"")) ||
                                (v.StartsWith("'") && v.EndsWith("'")))
                            {
                                v = v.Substring(1, v.Length - 2);
                            }

                            try
                            {
                                localKeys[k] = v;
                                props[k] = v;
                            }
                            catch
                            {
                                //ignore duplicates
                            }
                        }
                    }
                }
            }
            finally
            {
                stream.Close();
            }
        }
        resolveReferences(props);

        appLog.info("Loaded properties {0}: {1}", key, string.Join(";", localKeys));
    }

    protected void resolveReferences(Dictionary<String, String> props) {
        Regex var = new Regex("\\$\\{[^\\}]+\\}");
        StringBuilder newVar = new StringBuilder();

        Dictionary<String,String> modified = new Dictionary<string,string>();
        Dictionary<String,String>.Enumerator iter = props.GetEnumerator();
        while (iter.MoveNext())
        {
            MatchCollection match = var.Matches(iter.Current.Value);
            int lastPos = 0;
            foreach (Match m in match)
            {
                //appLog.info(String.format("match.group=%s", match.group()));
                String val;
                if (props.TryGetValue(m.Value.Replace("$","").Replace("{", "").Replace("}", ""), out val))
                {
                    newVar.Append(iter.Current.Value.Substring(lastPos, m.Index-lastPos));
                    newVar.Append(val);
                }
                lastPos = m.Index + m.Length;
                appLog.info("resolving var reference {0} to {1}", m.Value, val);
            }

            if (newVar.Length > 0) {
                appLog.info("Replacing updated property {0}={1}", iter.Current.Key, newVar);
                newVar.Append(iter.Current.Value.Substring(lastPos));
                modified[iter.Current.Key] = newVar.ToString();
                newVar.Clear();
            }
        }
        foreach (String k in modified.Keys)
            props[k] = modified[k];
    }

    protected void scheduleViewTask(long eventId) {
        if (minViewAfterInsert <= 0 || maxViewAfterInsert <= 0) return;

        int delay = (minViewAfterInsert + random.Next(maxViewAfterInsert - minViewAfterInsert));

        // implement warp-drive...
        if (timingSpeedup > 1) delay = (int)(delay / timingSpeedup);

        EventViewTask viewEvent = new EventViewTask(this, eventId);
        //queryExecutor.schedule(new EventViewTask(this, eventId), (long) delay, TimeUnit.SECONDS);
        queryExecutor.schedule(viewEvent, delay * Millis);

        appLog.info("Scheduled EventViewTask for now+{0}", delay);
    }

    internal class EventGenerator : Runnable 
    {

        private readonly long unique;
        private readonly Controller ctrl;
        private DateTime dateStamp = new DateTime();

        public EventGenerator(Controller ctrl, long unique) {
            this.unique = unique;
            this.ctrl = ctrl;
        }

        public void run() {

            long start = Environment.TickCount;

            long ownerId;
            long eventId;
            long groupId;

            //appLog.log("starting generation - getting first session...");

            // optionally start an enclosing session/transaction
            SqlSession outerTx = (ctrl.txModel == TxModel.UNIFIED ? new SqlSession(SqlSession.Mode.TRANSACTIONAL) : null);

            using (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                ownerId = generateOwner();
                Console.Out.WriteLine("\n------------------------------------------------");
                report("Owner", 1, Environment.TickCount - start);

                eventId = generateEvent(ownerId);
            }

            int groupCount = ctrl.minGroups + ctrl.random.Next(ctrl.maxGroups - ctrl.minGroups);
            appLog.info("Creating {0} groups", groupCount);

            int total = 2 + groupCount;

            Dictionary<String, Data> dataRows = new Dictionary<String, Data>(ctrl.maxData);

            // data records per group
            int dataCount = (ctrl.minData + ctrl.random.Next(ctrl.maxData - ctrl.minData)) / groupCount;
            appLog.info("Creating {0} Data records @ {1} records per group", dataCount * groupCount, dataCount);

            for (int gx = 0; gx < groupCount; gx++) {
                using (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                    groupId = generateGroup(eventId, gx);
                }

                total += dataCount;

                dataRows.Clear();
                for (int dx = 0; dx < dataCount; dx++) {
                    Data data = generateData(groupId, dx);
                    dataRows.Add(data.InstanceUID, data);
                }

                long uniquestart = Environment.TickCount;
                using (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                    long uniqueRows = ctrl.dataRepository.checkUniqueness(dataRows);

                    appLog.info("{0} rows out of {1} new rows are unique (check={2} ms)", uniqueRows, dataCount, Environment.TickCount - uniquestart);

                    long updateStart = Environment.TickCount;
                    ctrl.groupRepository.update(groupId, "dataCount", uniqueRows);
                    appLog.info("Group.datCount update; duration={0} ms", Environment.TickCount - updateStart);
                }
                appLog.info("Unique check time={0} ms", Environment.TickCount - uniquestart);

                long dataStart = Environment.TickCount;
                int count = 0;
                try {
                    using (SqlSession session = new SqlSession(ctrl.bulkCommitMode)) {
                        foreach (Data data in dataRows.Values) {
                            ctrl.dataRepository.persist(data);
                            count++;
                        }
                        appLog.info("inserting {0} data rows", count);
                    }
                } catch (Exception e) {
                    appLog.info("Error inserting data row {0}", e.ToString());
                }

                report("Data Group", dataCount, Environment.TickCount - dataStart);
            }

            // close the enclosing tx if it is open
            if (outerTx != null) outerTx.Dispose();

            long duration = Environment.TickCount - start;
            report("All Data", total, duration);

            //totalInserts += total;
            //totalInsertTime += duration;

            Interlocked.Add(ref ctrl.totalInserts, total);
            Interlocked.Add(ref ctrl.totalInsertTime, duration);

            ctrl.scheduleViewTask(eventId);
        }

        protected long generateOwner() {

            Owner owner = new Owner();
            owner.Name = String.Format("Owner-{0}", unique);
            owner.Region = (unique % 2 == 0 ? "Region_A" : "Region_B");

            return ctrl.ownerRepository.persist(owner);
        }

        protected long generateEvent(long ownerId) {

            Event ev = new Event();
            ev.Name = String.Format("Event-{0}", unique);
            ev.OwnerId = ownerId;
            ev.Date = dateStamp;
            ev.Region = (unique % 2 == 0 ? "Region_A" : "Region_B");

            return ctrl.eventRepository.persist(ev);
        }

        protected long generateGroup(long eventId, int index) {

            Group group = new Group();
            group.EventId = eventId;
            group.Name = String.Format("Group-{0}-{1}", unique, index);
            group.DataCount = 0;
            group.Date = dateStamp;
            group.Description = "Test data generated by CSNuoTest";
            group.Region = (unique % 2 == 0 ? "Region_A" : "Region_B");
            group.Week = (unique / 35000);

            return ctrl.groupRepository.persist(group);
        }

        protected Data generateData(long groupId, int index) {
            String suffix = string.Join("-", unique, groupId, index);
            String instanceUID = "image-"+suffix;
            
            Data data = new Data();
            data.GroupId = groupId;
            data.InstanceUID = instanceUID;
            data.Name = "Data-"+suffix;
            data.Description = "Test data generated by CSNuoTest";
            data.Path = "file:///remote/storage/"+instanceUID+".bin";
            data.RegionWeek = (unique % 2 == 0 ? "Region_A-" : "Region_B-") + (unique / 35000);

            return data;    // don't persist individually - we may be persisting in a batch
        }

        private void report(String name, int count, long duration) {
            double rate = (count > 0 && duration > 0 ? Millis2Seconds * count / duration : 0);
            appLog.info("Run {0}; generated {1} ({2} records); duration={3:N0} ms; rate={4:F2}", unique, name, count, duration, rate);
        }
    }

    internal class EventViewTask : Runnable 
    {
        private readonly long eventId;
        private readonly Controller ctrl;

        public EventViewTask(Controller ctrl, long eventId) {
            this.eventId = eventId;
            this.ctrl = ctrl;
        }

        public void run() {

            //long x = totalInserts;

            Controller.viewLog.info("Running view query for event {0}", eventId);

            try
            {
                using (SqlSession session = new SqlSession(SqlSession.Mode.READ_ONLY)) {

                    long start = Environment.TickCount;
                    EventDetails details = ctrl.eventRepository.getDetails(eventId);
                    long duration = Environment.TickCount - start;

                    if (details != null)
                    {
                        Interlocked.Increment(ref ctrl.totalQueries);
                        Interlocked.Add(ref ctrl.totalQueryRecords, details.Data.Count());
                        Interlocked.Add(ref ctrl.totalQueryTime, duration);

                        Controller.appLog.info("Event viewed. Query response time= {0:F2} secs; {1:N} Data objects attached in {2} groups.",
                                (duration / Controller.Millis2Seconds), details.Data.Count(), details.Groups.Count());
                    }
                }
            } catch (PersistenceException e) {
                Controller.viewLog.info("Error retrieving Event: {0}", e.ToString());
                //e.printStackTrace(System.out);
                Controller.viewLog.log(e.StackTrace.ToString());
            }

            //int queueSize = ((ThreadPoolExecutor) ctrl.insertExecutor).getQueue().size();
            //long queueSize = ctrl.totalScheduled - ctrl.totalInserts;
            long queueSize = ctrl.insertExecutor.QueueSize();
            if (ctrl.queryBackoff > 0 && queueSize > ctrl.maxQueued) {
                Controller.appLog.info("(query) Queue size > maxQueued ({0}); sleeping for {1} ms...", ctrl.maxQueued, ctrl.queryBackoff);
                Thread.Sleep(ctrl.queryBackoff);
            }
        }
    }

    }
}
