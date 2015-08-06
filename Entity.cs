using System;

namespace LoadItUp
{
    public abstract class Entity
    {
        internal long id { get { return id; } }
        internal bool persistent { get { return persistent; } }

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