/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.naming.factory.narayana;

import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;
import org.apache.tomcat.dbcp.dbcp2.PoolableConnection;
import org.apache.tomcat.dbcp.dbcp2.PoolableConnectionFactory;
import org.apache.tomcat.dbcp.dbcp2.managed.DataSourceXAConnectionFactory;
import org.apache.tomcat.dbcp.dbcp2.managed.ManagedDataSource;
import org.apache.tomcat.dbcp.pool2.impl.GenericObjectPool;
import org.apache.tomcat.dbcp.pool2.impl.GenericObjectPoolConfig;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.XADataSource;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * @author <a href="mailto:zfeng@redhat.com>Zheng Feng</a>
 */
public class TransactionalDataSourceFactory implements ObjectFactory {
    @Override
    public Object getObjectInstance(Object obj, Name name, Context context, Hashtable<?, ?> environment) throws Exception {

        if (obj == null || !(obj instanceof Reference)) {
            return null;
        }

        final Reference ref = (Reference) obj;
        if (!"javax.sql.DataSource".equals(ref.getClassName())) {
            return null;
        }

        TransactionManager transactionManager = (TransactionManager) getReferenceObject(ref, context, "transactionManager");
        XADataSource xaDataSource = (XADataSource) getReferenceObject(ref, context, "xaDataSource");

        XARecoveryModule xaRecoveryModule = getXARecoveryModule();
        if (xaRecoveryModule != null) {
            xaRecoveryModule.addXAResourceRecoveryHelper( new XAResourceRecoveryHelper() {
                @Override
                public boolean initialise(String p) throws Exception {
                    return true;
                }

                @Override
                public XAResource[] getXAResources() throws Exception {
                    try {
                        return new XAResource[] { xaDataSource.getXAConnection().getXAResource() };
                    } catch (SQLException ex) {
                        return new XAResource[0];
                    }
                }
            });
        }

        if (transactionManager != null && xaDataSource != null) {
            DataSourceXAConnectionFactory xaConnectionFactory =
                    new DataSourceXAConnectionFactory(transactionManager, xaDataSource);
            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(xaConnectionFactory, null);
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            GenericObjectPool<PoolableConnection> objectPool =
                    new GenericObjectPool<>(poolableConnectionFactory, config);
            poolableConnectionFactory.setPool(objectPool);
            return new ManagedDataSource<>(objectPool, xaConnectionFactory.getTransactionRegistry());
        } else {
            return null;
        }
    }

    private Object getReferenceObject(Reference ref, Context context, String prop) throws Exception {
        final RefAddr ra = ref.get(prop);
        if (ra != null) {
            return context.lookup(ra.getContent().toString());
        } else {
            return null;
        }
    }

    private XARecoveryModule getXARecoveryModule() {
        XARecoveryModule xaRecoveryModule = XARecoveryModule
                .getRegisteredXARecoveryModule();
        if (xaRecoveryModule != null) {
            return xaRecoveryModule;
        }
        throw new IllegalStateException(
                "XARecoveryModule is not registered with recovery manager");
    }
}

