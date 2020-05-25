//------------------------------------------------------------
//  Project parts
//
//  Created by Dmitry Bystrov.
//  Copyright 2013 E-STUDIO LLC, Inc. All rights reserved.
//------------------------------------------------------------

#ifndef DATABASE_H_F93E67C9_6863_4DFE_AF0B_5316F5614C4F
#define DATABASE_H_F93E67C9_6863_4DFE_AF0B_5316F5614C4F

#include "query_result.h"

namespace parts {

class CoreController;

namespace db {

class Query;
class QueryContext;

class Database : public ListenerBase {
  friend class parts::CoreController;
  friend class parts::db::Query;
  friend class parts::db::QueryContext;

 public:
  static Database* GetInstance();

 public:
  bool                IsCorrupted() const;
  void                Repair();
  QueryResultPointer  ExecuteQuery(const std::string& sQueryString);
  QueryResultPointer  ExecuteQuery(const nE_DataTable& queryTable);
  QueryResultPointer  ExecuteQuery(const nE_DataTable* pQueryTable);
  QueryResultPointer  ExecuteQuery(const nE_DataTablePointer pQueryTable);
  bool                ExecuteQueryArray(const nE_DataArray* pQueryArray,
                                        QueryResultVector* pQueryResultVector = NULL);
  nE_DataArrayPointer CreateDump(const nE_DataTable* pDumpTable);
  bool                ApplyDump(const nE_DataArray* pDumpArray);
  CollectionPointer   GetCollection(const std::string& sCollectionName) const;
  void                RegisterReadonlyCollections(nE_DataArray* pCollections);

 protected:
  static void ScriptExecuteQuery(nE_DataArray* pArgs, void* pUserBoundData,
                                 nE_DataArray* pResult);
  static void ScriptRegisterReadonlyCollections(nE_DataArray* pArgs,
      void* pUserBoundData, nE_DataArray* pResult);

 protected:
  void Handle_Command_SaveState(nE_DataTable* pTable);
  void Handle_Event_HeartBeat(nE_DataTable* pTable);

  INVOKE_MAP_BEGIN
  INVOKE_HANDLER(Messages::Command_SaveState, Handle_Command_SaveState)
  INVOKE_HANDLER(Messages::Event_HeartBeat, Handle_Event_HeartBeat)
  INVOKE_MAP_END

 protected:
  typedef std::map<std::string, CollectionPointer> CollectionMap;
  typedef std::pair<std::string, CollectionPointer> CollectionMapPair;

 protected:
  Database(const nE_DataTable* pOptionTable);
  Database(const Database& database);
  Database& operator=(const Database& database);
  virtual            ~Database();
  static void        Initialize(const nE_DataTable* pOptionTable);
  static void        Destroy();
  void               InitializeSystemCollections();
  nE_DataPointer     ReadCollectionData(const std::string& sCollectionFilePath,
                                        bool bIsEncoded);
  void               InitializeReadonlyCollections(const nE_DataTable*
      pOptionTable);

  bool               RegisterNewReadonlyCollections(const nE_DataArray*
      pCollectionFileNames);
  void               RegisterBaseReadonlyCollections(const nE_DataTable*
      pOptionTable);

  void               LoadReadonlyCollections();
  void               ReloadReadonlyCollections();

  std::string        CreateReadonlyCollection(nE_DataPointer pData);

  void               InitializeWritableCollections(const nE_DataTable*
      pOptionTable);
  std::string        CreateWritableCollection(nE_DataPointer pData);

  std::string        CreateTemporaryCollection(nE_DataPointer pData);
  void               GenerateTemporaryCollectionName(std::string&
      sCollectionName);

  virtual bool       LoadWritableCollections();
  virtual void       SaveWritableCollections();

  void               Load(void);
  void               CompleteLoading();

  QueryResultPointer ExecuteQueryInternal(const nE_Data* pQueryData,
                                          QueryContext& queryContext);

 protected:
  static Database*   s_pInstance;
  bool               m_bIsCorrupted;
  bool               m_bIsReady;
  CollectionMap      m_Collections;
  nE_DataTable       m_ReadonlyCollectionOptions;
  nE_StringVector    m_vReadonlyCollections;
  int                m_iNextTemporaryCollection;
};

}
}

#include "database-inl.h"

#endif//DATABASE_H_F93E67C9_6863_4DFE_AF0B_5316F5614C4F