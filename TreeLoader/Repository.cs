using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadItUp
{
    interface Repository<T> where T : Entity
    {
        public void init();

        public T findById(long id);

        public List<T> findAllBy(String column, params Object[] values);

        public String getValue(String column, String criteria);

        public long persist(T entity);

        public void update(long id, String columns, params Object[] values); 
    }
}
