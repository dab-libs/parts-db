//------------------------------------------------------------
//  Project parts
//
//  Created by Dmitry Bystrov.
//  Copyright 2013 E-STUDIO LLC, Inc. All rights reserved.
//------------------------------------------------------------

#include "parts/include.h"
#include "database.h"
#include "collection.h"
#include "query_context.h"
#include "query_builder.h"
#include "query.h"
#include "parts/storage/storage.h"
#include "parts/version/version.h"
#include "parts/net/net.h"
#include <memory.h>

namespace parts {
namespace db {

Database* Database::s_pInstance = NULL;

Database::Database(const nE_DataTable* pOptionTable)
  : m_iNextTemporaryCollection(0)
  , m_bIsCorrupted(false)
  , m_bIsReady(false) {
  InitializeListener();

  InitializeSystemCollections();
  InitializeReadonlyCollections(pOptionTable);
  InitializeWritableCollections(pOptionTable);
}

Database::~Database(void) {
}

void Database::Initialize(const nE_DataTable* pOptionTable) {
  if (s_pInstance == NULL) {
    s_pInstance = new Database(pOptionTable);
  }
  nE_ScriptFuncHub::RegisterFunc("DbExecuteQuery; db_execute_query",
                                 ScriptExecuteQuery, s_pInstance);
  nE_ScriptFuncHub::RegisterFunc("parts.db.RegisterReadonlyCollections; DbRegisterReadonlyCollections",
                                 ScriptRegisterReadonlyCollections, s_pInstance);
}

void Database::Destroy() {
  if (s_pInstance != NULL) {
    delete s_pInstance;
    s_pInstance = NULL;
  }
}

Database* Database::GetInstance() {
  return s_pInstance;
}

bool Database::IsCorrupted() const {
  return m_bIsCorrupted;
}

void Database::Repair() {
  m_bIsCorrupted = false;
}

QueryResultPointer Database::ExecuteQuery(const std::string& sQueryString) {
  nE_DataPointer pQuery(nE_DataUtils::LoadDataFromJsonString(sQueryString));
  QueryContext queryContext;
  return ExecuteQueryInternal(pQuery.get(), queryContext);
}

QueryResultPointer Database::ExecuteQuery(const nE_DataTable& queryTable) {
  QueryContext queryContext;
  return ExecuteQueryInternal(&queryTable, queryContext);
}

QueryResultPointer Database::ExecuteQuery(const nE_DataTable* pQueryTable) {
  QueryContext queryContext;
  return ExecuteQueryInternal(pQueryTable, queryContext);
}

QueryResultPointer Database::ExecuteQuery(const nE_DataTablePointer
    pQueryTable) {
  QueryContext queryContext;
  return ExecuteQueryInternal(pQueryTable.get(), queryContext);
}

bool Database::ExecuteQueryArray(const nE_DataArray* pQueryArray,
                                 QueryResultVector* pQueryResultVector) {
  bool bHasErrors = false;
  for (size_t i = 0; i < pQueryArray->Size(); i++) {
    const nE_DataTable* pQueryTable = pQueryArray->Get(i)->AsTable();
    QueryResultPointer pQueryResult = ExecuteQuery(pQueryTable);
    bHasErrors |= pQueryResult->HasErrors();
    if (pQueryResultVector != NULL) {
      pQueryResultVector->push_back(pQueryResult);
    }
  }
  return (!bHasErrors);
}

void Database::ScriptExecuteQuery(nE_DataArray* pArgs, void* pUserBoundData,
                                  nE_DataArray* pResult) {
  Database* pThis = (Database*) pUserBoundData;
  nE_Data* pQuestTable = pArgs->Get(0);
  QueryContext queryContext;
  QueryResultPointer pQueryResult = pThis->ExecuteQueryInternal(pQuestTable,
                                    queryContext);
  nE_DataTable* pResultTable = pResult->PushNewTable();
  if (!pQueryResult->HasErrors()) {
    pResultTable->Push("status", 1);
    pResultTable->PushCopy("result", pQueryResult->GetResult().get());
  }
  else {
    pResultTable->Push("status", 0);
    std::string sErrors(pQueryResult->GetErrors());
    pResultTable->Push("errors", sErrors);
    nE_Log::Write(sErrors.c_str());
  }
}

void Database::ScriptRegisterReadonlyCollections(nE_DataArray* pArgs,
    void* pUserBoundData, nE_DataArray* pResult) {
  GetInstance()->RegisterReadonlyCollections(pArgs->Get(0)->AsArray());
}

void Database::InitializeSystemCollections() {
  nE_DataTable collectionOptions;
  collectionOptions.Push("name", "parts/db");
  nE_DataTable* pIndices = collectionOptions.PushNewTable("indices");
  pIndices->Push("name", "name");
  collectionOptions.PushNewArray("items");
  CreateWritableCollection(nE_DataPointer(collectionOptions.Clone()));
}

nE_DataPointer Database::ReadCollectionData(const std::string&
    sCollectionFilePath, bool bIsEncoded) {
  nE_FileManager::FileAttributes attributes;
  std::string sFilePath(sCollectionFilePath);
  if (bIsEncoded) {
    sFilePath += ".dat";
    attributes = nE_FileManager::FA_ChecksumDecoded;
  }
  else {
    sFilePath += ".json";
    attributes = nE_FileManager::FA_Null;
  }
  nE_Data* pData = nE_DataUtils::LoadDataFromJsonFile(sFilePath, attributes);
  return nE_DataPointer(pData);
}

void Database::InitializeReadonlyCollections(const nE_DataTable*
    pOptionTable) {
  std::string sDirectory(
    nE_DataUtils::GetAsString(pOptionTable, "directory", ""));

  const nE_DataArray* pCollectionFileNames =
    nE_DataUtils::GetAsArrayNotNull(pOptionTable, "collections");

  if (pOptionTable != &m_ReadonlyCollectionOptions) {
    m_ReadonlyCollectionOptions.Push("directory", sDirectory);
    m_ReadonlyCollectionOptions.PushCopy("collections", pCollectionFileNames);
  }
}

bool Database::RegisterNewReadonlyCollections(const nE_DataArray*
    pCollectionFileNames) {
  if (m_vReadonlyCollections.size() > 0) {
    bool bChanged = false;
    for (size_t i = 0; i < pCollectionFileNames->Size(); ++i) {
      const std::string sCollection = pCollectionFileNames->Get(i)->AsString();
      std::vector<std::string>::const_iterator it = std::find(
            m_vReadonlyCollections.begin(), m_vReadonlyCollections.end(), sCollection);
      if (it == m_vReadonlyCollections.end()) {
        m_vReadonlyCollections.push_back(sCollection);
        bChanged = true;
      }
    }
    return bChanged;
  }
  else {
    for (size_t i = 0; i < pCollectionFileNames->Size(); ++i) {
      m_vReadonlyCollections.push_back(pCollectionFileNames->Get(i)->AsString());
    }
    return true;
  }
}

void Database::RegisterBaseReadonlyCollections(const nE_DataTable*
    pOptionTable) {
  std::string sDirectory(
    nE_DataUtils::GetAsString(pOptionTable, "directory", ""));

  const nE_DataArray* pCollectionFileNames =
    nE_DataUtils::GetAsArrayNotNull(pOptionTable, "collections");

  nE_DataArray collectionFilePath;
  for (size_t i = 0; i < pCollectionFileNames->Size(); ++i) {
    std::string sCollectionFile =
      nE_DataUtils::GetAsStringException(
        pCollectionFileNames->Get(i), "",
        "Error: The config option 'collections' must an array of strings.");
    collectionFilePath.Push(sDirectory + sCollectionFile);
  }
  RegisterNewReadonlyCollections(&collectionFilePath);
}

void Database::LoadReadonlyCollections() {
  for (auto it = m_vReadonlyCollections.begin();
       it != m_vReadonlyCollections.end(); ++it) {
    nE_DataPointer pData(ReadCollectionData(*it, false));
    if (pData != (nE_DataPointer)NULL) {
      CreateReadonlyCollection(pData);
    }
  }
}

void Database::ReloadReadonlyCollections() {
  for (auto it = m_Collections.begin(); it != m_Collections.end();) {
    if (it->second->IsReadOnly()) {
      m_Collections.erase(it++);
    }
    else {
      ++it;
    }
  }
  LoadReadonlyCollections();
}

std::string Database::CreateReadonlyCollection(nE_DataPointer pData) {
  CollectionPointer pNewCollection(new Collection());
  pNewCollection->SetCollectionData(pData);
  std::string sCollectionName(pNewCollection->GetName());
  CollectionPointer pCollection = GetCollection(sCollectionName);
  if (pCollection ==(CollectionPointer) NULL) {
    m_Collections.insert(CollectionMapPair(pNewCollection->GetName(),
                                           pNewCollection));
  }
  else {
    pCollection->AppendCollection(pNewCollection);
  }
  return sCollectionName;
}

void Database::InitializeWritableCollections(const nE_DataTable* pOptionTable) {
  const nE_DataArray* pWritableCollections = nE_DataUtils::GetAsArrayNotNull(
        pOptionTable, "writable_collections");
  for (size_t i = 0; i < pWritableCollections->Size(); ++i) {
    const nE_Data* pData =
      nE_DataUtils::GetAsTableException(
        pWritableCollections->Get(i), "",
        "Error: The config option 'writable_collections' must be an array of tables.");
    CreateWritableCollection(nE_DataPointer(pData->Clone()));
  }
}

std::string Database::CreateWritableCollection(nE_DataPointer pData) {
  CollectionPointer pCollection(new Collection());
  pCollection->SetReadOnly(false);
  pCollection->SetCollectionData(pData);
  m_Collections.insert(CollectionMapPair(pCollection->GetName(), pCollection));
  return pCollection->GetName();
}

std::string Database::CreateTemporaryCollection(nE_DataPointer pData) {
  std::string sCollectionName;
  GenerateTemporaryCollectionName(sCollectionName);
  pData->AsTable()->Push("name", sCollectionName);
  return CreateWritableCollection(pData);
}

void Database::GenerateTemporaryCollectionName(std::string& sCollectionName) {
  char sNameBuffer[ 30 ] = "";
  int nNameBufferSize = sprintf(sNameBuffer, "temp%020d",
                                m_iNextTemporaryCollection++);
  sNameBuffer[(nNameBufferSize > 0 ? nNameBufferSize : 0) ] = 0;
  sCollectionName = sNameBuffer;
}

QueryResultPointer Database::ExecuteQueryInternal(const nE_Data* pQueryData,
    QueryContext& queryContext) {
  Query query(this, &queryContext);
  nE_DataPointer pResult = query.Execute(pQueryData);
  if (queryContext.GetErrorStorage().IsEmpty()) {
    return QueryResultPointer(new QueryResult(pResult));
  }
  else {
    std::string sQuery;
    nE_DataUtils::SaveDataToJsonString(pQueryData, sQuery, true);
    std::string sError = "In query: ";
    sError += sQuery;
    queryContext.GetErrorStorage().Add(sError);
    return QueryResultPointer(new QueryResult(
                                queryContext.GetErrorStorage().GetErrorMessage()));
  }
}

CollectionPointer Database::GetCollection(const std::string& sCollectionName)
const {
  CollectionMap::const_iterator it = m_Collections.find(sCollectionName);
  if (it != m_Collections.end()) {
    return it->second;
  }
  else {
    return CollectionPointer();
  }
}

void Database::RegisterReadonlyCollections(nE_DataArray* pCollections) {
  bool bIsRegistered =
    RegisterNewReadonlyCollections(pCollections);
  if (bIsRegistered) {
    ReloadReadonlyCollections();
  }
}

bool Database::LoadWritableCollections() {
  bool bResult = true;
  nE_DataTable writableCollectionItems;
  CollectionMap::iterator it = m_Collections.begin();
  for (; bResult && it != m_Collections.end(); ++it) {
    CollectionPointer pCollection = it->second;
    if (pCollection->IsReadOnly() ||
        !storage::Storage::GetInstance()->DataExists(pCollection->GetName())) {
      continue;
    }

    std::string sJsonItems;
    bResult = (storage::Storage::GetInstance()->ReadData(pCollection->GetName(),
               sJsonItems) == storage::StorageResult::OK);
    if (!bResult) {
      break;
    }

    nE_Data* pItems = nE_DataUtils::LoadDataFromJsonString(sJsonItems);
    bResult = (pItems != NULL && pItems->AsArray() != NULL);
    if (bResult) {
      nE_DataArray* pItemArray = pItems->AsArray();
      for (size_t i = 0; bResult && i < pItemArray->Size(); ++i) {
        bResult = (pItemArray->Get(i) != NULL &&
                   pItemArray->Get(i)->GetType() == nE_Data::Data_Table);
      }
    }

    if (bResult) {
      writableCollectionItems.Push(pCollection->GetName(), pItems);
    }
    else {
      delete pItems;
    }
  }

  if (bResult) {
    nE_DataTableIterator it = writableCollectionItems.Begin();
    for (; it != writableCollectionItems.End(); ++it) {
      CollectionPointer pCollection = db::Database::GetInstance()->GetCollection(
                                        it.Key());
      pCollection->DeleteAll();
      nE_DataArray* pItemArray = it.Value()->AsArray();
      for (size_t i = 0; i < pItemArray->Size(); ++i) {
        pCollection->InsertItem(pItemArray->Get(i)->AsTable());
      }
      pCollection->ResetChanges();
    }
  }

  return bResult;
}

void Database::SaveWritableCollections() {
  CollectionMap::iterator it = m_Collections.begin();
  for (; it != m_Collections.end(); ++it) {
    CollectionPointer pCollection = it->second;
    if (pCollection !=(CollectionPointer) NULL && pCollection->IsChanged()) {
      std::string sJsonItems;
      nE_DataUtils::SaveDataToJsonString(pCollection->GetItems(), sJsonItems, true);
      storage::Storage::GetInstance()->WriteData(pCollection->GetName(), sJsonItems);
      pCollection->ResetChanges();
    }
  }
}

void Database::Load(void) {
  if (LoadWritableCollections()) {
    RegisterBaseReadonlyCollections(&m_ReadonlyCollectionOptions);
    LoadReadonlyCollections();
  }
  else {
    m_bIsCorrupted = true;
  }
  CompleteLoading();
}

void Database::CompleteLoading(void) {
  if (!m_bIsReady) {
    m_bIsReady = true;
    nE_Mediator::GetInstance()->SendMessage(Messages::Event_Db_Ready, NULL);
  }
}

void Database::Handle_Command_SaveState(nE_DataTable* pTable) {
  SaveWritableCollections();
}

void Database::Handle_Event_HeartBeat(nE_DataTable* pTable) {
  if (!m_bIsReady) {
    Load();
  }
}

nE_DataArrayPointer Database::CreateDump(const nE_DataTable* pDumpTable) {
  nE_DataArrayPointer pDumpArray;
  pDumpArray.reset(new nE_DataArray());

  nE_DataTableConstIterator it = pDumpTable->Begin();
  for (; it != pDumpTable->End(); it++) {
    CollectionPointer pCollection = GetCollection(it.Key());
    if (pCollection !=(CollectionPointer) NULL) {
      nE_DataTable* pCollectionDump = pDumpArray->PushNewTable();
      pCollectionDump->Push("query", "insert");
      pCollectionDump->Push("collection", it.Value()->AsString());
      pCollectionDump->PushCopy("value", pCollection->GetItems());
    }
  }
  return pDumpArray;
}

bool Database::ApplyDump(const nE_DataArray* pDumpArray) {
  for (size_t i = 0; i < pDumpArray->Size(); i++) {
    QueryResultPointer pQueryResult = Database::GetInstance()->ExecuteQuery(
                                        pDumpArray->Get(i)->AsTable());
    if (pQueryResult->HasErrors()) {
      return false;
    }
  }
  return true;
}

}
}

