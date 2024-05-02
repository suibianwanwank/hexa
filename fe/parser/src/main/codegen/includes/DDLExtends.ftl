<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

SqlNode ExplainSqlPlan():
{
    SqlParserPos pos;
    SqlNode sqlStatement;
}
{
    <EXPLAIN> { pos = getPos(); }
          sqlStatement = SqlQueryOrDml()
          {
             return new SqlProfileExplain(pos, sqlStatement);
          }
}

SqlNode SetConfig():
{
    SqlParserPos pos;
    SqlIdentifier scope = null;
    SqlKvEntry options = null;
}
{
    <SET> { pos = getPos(); }
    [
        (
            <GLOBAL>  { scope = new SqlIdentifier("GLOBAL", getPos()); }
            |
            <CLUSTER> { scope = new SqlIdentifier("CLUSTER", getPos()); }
            |
            <CATALOG> { scope = new SqlIdentifier("CATALOG", getPos()); }
            |
            <SESSION> { scope = new SqlIdentifier("SESSION", getPos()); }
         )
    ]
          options = readSingleKV()
    {
          return new SqlSetConfig(pos, scope, options);
    }
}


<#--parser for  key value statement-->
SqlKvEntry readSingleKV():
{
    SqlParserPos pos;
    SqlNode key;
    SqlNode value;
}
{
    { pos = getPos(); }
    (
        key = CompoundIdentifier()
    |
        key = StringLiteral()
    )
    (
        <EQ>
        (
            value = NumericLiteral()
        |
            value = SimpleIdentifier()
        |
            value = SpecialLiteral()
        |
            value = StringLiteral()
        )
        |
        <COLON>
        (
            value = NumericLiteral()
        |
            value = SimpleIdentifier()
        |
            value = SpecialLiteral()
        |
            value = StringLiteral()
        )
    )

    {
        return new SqlKvEntry(pos, key, value);
    }
}

SqlNode ShowMetadata():
{
    SqlParserPos pos;
    SqlIdentifier sourceName = null;
}
{
   <SHOW> { pos = getPos(); }
    (
       <CATALOGS>
           {
                return new SqlShowCatalogs(pos);
           }
      |
       <SCHEMAS> <FROM> sourceName = CompoundIdentifier()
           {
               return new SqlShowSchemas(pos, sourceName);
           }
       |
       <CREATE> <CATALOG> sourceName = CompoundIdentifier()
           {
               return new SqlShowCreateCatalog(pos, sourceName);
           }
       |
       <TABLES> <FROM> sourceName = CompoundIdentifier()
           {
               return new SqlShowTables(pos, sourceName);
           }
       |
       <COLUMNS> <FROM> sourceName = CompoundIdentifier()
           {
               return new SqlShowColumns(pos, sourceName);
           }
       |
       <PROFILE> sourceName = CompoundIdentifier()
           {
               return new SqlShowProfile(pos, sourceName);
           }
    )
}

SqlNode RefreshMetadata():
{
    SqlParserPos pos;
    SqlIdentifier sourceName = null;
}
{
   <REFRESH> { pos = getPos(); }
    (
       <TABLE> sourceName = CompoundIdentifier()
           {
               return new SqlRefreshTable(pos, sourceName);
           }
    )
}
SqlNode SqlCreateCatalog():
{
    SqlParserPos pos;
    boolean ifNotExists =false;
    SqlIdentifier datasourceType;
    SqlIdentifier catalogName;
    SqlParserPos attPos;
    String connectUrl = null;
    SqlNodeList properties = null;
}
{
    <CREATE>
    {
      pos = getPos();
    }
    <CATALOG>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
      datasourceType = CompoundIdentifier()
    <AS>
        catalogName = CompoundIdentifier()
    [
        <WITH> properties = parseCommaPropertiesList()
    ]
    {
        return new SqlCreateCatalog(pos, ifNotExists, datasourceType,
                catalogName, properties);
    }
}

SqlNodeList parseCommaPropertiesList():
{
    SqlNodeList properties;
}
{
    <LBRACE>
        {
            properties = new SqlNodeList(getPos());
            properties.add(readSingleKV());
        }
        (
            <COMMA> { properties.add(readSingleKV()); }
        )*
    <RBRACE>
    { return properties; }
}