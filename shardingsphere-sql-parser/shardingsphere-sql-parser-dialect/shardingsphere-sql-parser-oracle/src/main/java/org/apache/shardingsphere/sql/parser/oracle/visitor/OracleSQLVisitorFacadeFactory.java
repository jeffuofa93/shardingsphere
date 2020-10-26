/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sql.parser.oracle.visitor;

import org.apache.shardingsphere.sql.parser.api.visitor.SQLVisitorFacadeFactory;
import org.apache.shardingsphere.sql.parser.api.visitor.operation.format.SQLFormatVisitorFacade;
import org.apache.shardingsphere.sql.parser.api.visitor.operation.statement.SQLStatementVisitorFacade;
import org.apache.shardingsphere.sql.parser.oracle.visitor.format.facade.OracleFormatSQLVisitorFacade;
import org.apache.shardingsphere.sql.parser.oracle.visitor.statement.facade.OracleStatementSQLVisitorFacade;

/**
 * Oracle SQL visitor facade engine.
 */
public final class OracleSQLVisitorFacadeFactory implements SQLVisitorFacadeFactory {
    
    @Override
    public Class<? extends SQLStatementVisitorFacade> getStatementSQLVisitorFacadeClass() {
        return OracleStatementSQLVisitorFacade.class;
    }
    
    @Override
    public Class<? extends SQLFormatVisitorFacade> getFormatSQLVisitorFacadeClass() {
        return OracleFormatSQLVisitorFacade.class;
    }
}
