using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class ConfigurationException : Exception
    {
        public ConfigurationException(String msg, params Object[] args)
            : base(String.Format(msg, args))
        { }
    }
}
