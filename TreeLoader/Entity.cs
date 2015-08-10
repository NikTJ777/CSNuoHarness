using System;

namespace NuoTest
{
    public abstract class Entity
    {
        internal long id { get; set;  }
        internal bool persistent { get; set;  }

	    protected Entity(long id)
	    {
            this.id = id;
            persistent = true;
	    }

        public Entity()
        {
            id = 0;
            persistent = false;
        }
    }
}