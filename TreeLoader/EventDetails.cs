using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class EventDetails {

        internal Event Event { get; set; }
        internal Owner Owner { get; set; }
        internal List<Group> Groups { get; set; }
        internal List<Data> Data { get; set; }

        public EventDetails(Event ev, Owner owner) {
            this.Event = ev;
            this.Owner = owner;
        }

    }
}
