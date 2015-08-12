using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuoTest
{
    interface Repository<T> where T : Entity
    {
        void init();

        T findById(long id);

        List<T> findAllBy(String column, params Object[] values);

        String getValue(String column, String criteria);

        long persist(T entity);

        void update(long id, String columns, params Object[] values); 
    }
}
