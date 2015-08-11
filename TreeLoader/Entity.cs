using System;

namespace NuoTest
{
    public abstract class Entity
    {
        internal long Id { get; set;  }
        internal bool Persistent { get; set;  }

	    protected Entity(long id)
	    {
            this.Id = id;
            Persistent = true;
	    }

        public Entity()
        {
            Id = 0;
            Persistent = false;
        }
    }
}