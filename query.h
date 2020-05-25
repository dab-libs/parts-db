//------------------------------------------------------------
//  Project parts
//
//  Created by Dmitry Bystrov.
//  Copyright 2013 E-STUDIO LLC, Inc. All rights reserved.
//------------------------------------------------------------

#ifndef QUERY_H_44CBC845_F827_4A69_A1C5_23967A144E10
#define QUERY_H_44CBC845_F827_4A69_A1C5_23967A144E10

#include "data_reference.h"

namespace parts {
namespace db {

class Database;
class QueryContext;
class ErrorStorage;

class Query {
 public:
  Query(Database* pDatabase, QueryContext* pQueryContext);
  virtual ~Query();
  nE_DataPointer Execute(const nE_Data* pQueryTable);
  static bool MayBeQueryTable(const nE_Data* pQueryTable);

 public:
  class ParsedQuery {
   public:
    QueryContext*                  m_pQueryContext;
    std::string                    m_sQueryType;
    std::string                    m_sCollectionName;
    std::string                    m_sIndexName;
    CollectionPointer              m_pCollection;
    ReadonlyCollectionIndexPointer m_pIndex;
    std::string                    m_sAlias;
    const nE_DataTable*            m_pCriteria;
    const nE_Data*                 m_pResult;
    const nE_Data*                 m_pValue;
    const nE_DataTable*            m_pSet;
    const nE_DataTable*            m_pIndices;
    const nE_DataArray*            m_pCrypts;
    const nE_DataArray*            m_pItems;

    ParsedQuery(QueryContext* pQueryContext);
    bool Parse(const nE_DataTable* pQueryTable, Database& database,
               ErrorStorage& errorStorage);
    bool ParseCommon(const nE_DataTable* pQueryTable, Database& database,
                     ErrorStorage& errorStorage);
    bool ParseFind(const nE_DataTable* pQueryTable, Database& database,
                   ErrorStorage& errorStorage);
    bool ParseWritable(const nE_DataTable* pQueryTable, Database& database,
                       ErrorStorage& errorStorage);
    bool ParseInsert(const nE_DataTable* pQueryTable, Database& database,
                     ErrorStorage& errorStorage);
    bool ParseUpdate(const nE_DataTable* pQueryTable, Database& database,
                     ErrorStorage& errorStorage);
    bool ParseDelete(const nE_DataTable* pQueryTable, Database& database,
                     ErrorStorage& errorStorage);
    bool ParseCreate(const nE_DataTable* pQueryTable, Database& database,
                     ErrorStorage& errorStorage);
  };

 private:
  typedef std::vector<const nE_DataTable*> ItemVector;

 private:
  nE_Data* Find(const ParsedQuery& parsedQuery);
  nE_Data* FindAll(const ParsedQuery& parsedQuery, size_t iLimit = INT_MAX);
  nE_Data* Update(const ParsedQuery& parsedQuery);
  nE_Data* UpdateAll(const ParsedQuery& parsedQuery, size_t iLimit = INT_MAX);
  nE_Data* Insert(const ParsedQuery& parsedQuery);
  nE_Data* Delete(const ParsedQuery& parsedQuery);
  nE_Data* DeleteAll(const ParsedQuery& parsedQuery, size_t iLimit = INT_MAX);
  nE_Data* Create(const ParsedQuery& parsedQuery);
  nE_Data* CreateIfNotExists(const ParsedQuery& parsedQuery);

 private:
  void FindItems(const ParsedQuery& parsedQuery, size_t iLimit,
                 ItemVector& items);
  void FindAllAll(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                  ItemVector& items);
  void FindAllLike(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                   const nE_Data* pLike, ItemVector& items);
  void FindAllMinMax(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                     const nE_Data* pMin, const nE_Data* pMax, ItemVector& items);
  void FindAllIn(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                 nE_Data* pIn, ItemVector& items);

 private:
  nE_Data* FindResult(const ParsedQuery& parsedQuery,
                      const nE_Data* pCollectionItem);
  void UpdateItem(const ParsedQuery& parsedQuery, const nE_Data* pCollectionItem);
  void SendCollectionUpdated(const ParsedQuery& parsedQuery);

 private:
  Database* m_pDatabase;
  QueryContext* m_pQueryContext;
};

typedef std::shared_ptr<Query> QueryPointer;

inline bool IsString(const nE_Data* pData) {
  return (pData != NULL && pData->GetType() == nE_Data::Data_String);
}

inline bool IsTable(const nE_Data* pData) {
  return (pData != NULL && pData->GetType() == nE_Data::Data_Table);
}

}
}

#endif//QUERY_H_44CBC845_F827_4A69_A1C5_23967A144E10
