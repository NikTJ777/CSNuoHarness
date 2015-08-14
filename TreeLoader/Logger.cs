using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/**
 * A simple Logger class.
 * 
 * Apparently C# doesn't come with one built in??
 * Furthermore, it appears that String.Format() is rather slow, so we avoid where we can.
 */
namespace NuoTest
{
    class Logger
    {
        internal static Dictionary<String, Logger> extent = new Dictionary<String, Logger>(32);

        internal readonly String name;

        private Logger(String name)
        {
            this.name = name;
        }

        public static Logger getLogger(String name)
        {
            Logger result;
            if (!extent.TryGetValue(name, out result))
            {
                result = new Logger(name);
                extent[name] = result;
            }

            return result;
        }

        public void log(String msg)
        { write(msg); }

        public void info(String msg, params Object[] args)
        { write("INFO", msg, args); }

        protected void write(String level, String msg, params Object[] args)
        { write(level, String.Format(msg, args)); }

        protected void write(String msg)
        {
            Console.Write(DateTime.Now.ToString("MMM dd, yyyy hh:mm:ss tt "));
            Console.WriteLine(msg);
        }

        protected void write(String level, String msg)
        {
            StringBuilder builder = new StringBuilder(DateTime.Now.ToString("MMM dd, yyyy hh:mm:ss tt "))
                .Append(Thread.CurrentThread.Name)
                .Append(" - ")
                .Append(name)
                .Append("\n")
                .Append(level)
                .Append(": ")
                .Append(msg);

            Console.WriteLine(builder.ToString());
        }
    }
}
