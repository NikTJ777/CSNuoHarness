using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    class EventRepository : AbstractRepository<Event>
    {
        private readonly OwnerRepository ownerRepository;
        private readonly GroupRepository groupRepository;
        private readonly DataRepository dataRepository;

        //internal static Logger log = Logger.getLogger("EventRepository");

        public EventRepository(OwnerRepository ownerRepository, GroupRepository groupRepository, DataRepository dataRepository)
            : base("NuoTest.T_EVENT", "ownerId", "name", "description", "date", "region")
        {
            this.ownerRepository = ownerRepository;
            this.groupRepository = groupRepository;
            this.dataRepository = dataRepository;
        }

        //@Override
        public override void init()
        {
            if (ownerRepository == null || groupRepository == null || dataRepository == null)
                throw new ConfigurationException("Dependencies have not been set: ownerRepository {0}; groupRepository {1}; dataRepository {2}.",
                        ownerRepository, groupRepository, dataRepository);

        }

        /**
         * Retrieve the details of an Event and all its associated objects.
         *
         * @param eventId
         * @return an EventDetails object that has references to associated Owner, Groups, and Data
         *
         * @throws PersistenceException if there is any error in retrieving the data.
         */
        public EventDetails getDetails(long eventId)
        {
            EventDetails result = null;
            try {

                Event ev = findById(eventId);
                Owner owner = ownerRepository.findById(ev.OwnerId);
                result = new EventDetails(ev, owner);

                List<Group> groups = groupRepository.findAllBy("eventId", eventId);
                result.Groups = groups;

                log.info("retrieved {0} groups", groups.Count());

                long[] groupIds = new long[groups.Count()];
                for (int gx = 0; gx < groupIds.Length; gx++) {
                    groupIds[gx] = groups[gx].Id;
                }

                List<Data> data = dataRepository.findAllBy("groupId", groupIds);
                result.Data = data;

                log.info("retrieved {0} data records", data.Count());

            } catch (Exception e) {
                //e.StackTrace.ToString();
                log.info("getDetails exception: {0}\n{1}", e.ToString(), e.StackTrace.ToString());
                throw new PersistenceException(e, "Error retrieving EventView {0}", eventId);
            }

            return result;
        }

        //@Override
        protected override Event mapIn(DbDataReader row)
        {
            Event ev = new Event(row.GetInt64(0), row.GetString(2));
            ev.OwnerId = row.GetInt64(1);
            ev.Description = row.GetString(3);
            ev.Date = row.GetDateTime(4);
            ev.Region = row.GetString(5);

            return ev;
        }

        //@Override
        protected override DataRow mapOut(Event ev, SqlSession session)
        {
            DataTable table;
            if (! session.BatchTable.TryGetValue(tableName, out table))
            {
                table = new System.Data.DataTable(tableName);
                session.BatchTable[tableName] = table;

                table.Columns.Add("ownerId", typeof(long));
                table.Columns.Add("name", typeof(String));
                table.Columns.Add("description", typeof(String));
                table.Columns.Add("date", typeof(DateTime));
                table.Columns.Add("region", typeof(String));
            }

            DataRow row = table.NewRow();

            row[0] = ev.OwnerId;
            row[1] = ev.Name;
            row[2] = ev.Description;
            row[3] = ev.Date;
            row[4] = ev.Region;

            return row;
        }

    }
}
