using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{
    class PersistenceException : Exception
    {
        public PersistenceException(Exception e, String message, params Object[] values)
            : base(String.Format(message, values), e)
        {}

        public PersistenceException(String message, params Object[] values)
            : base(String.Format(message, values))
        {}
    }
}
