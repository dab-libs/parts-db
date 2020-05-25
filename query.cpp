//------------------------------------------------------------
//  Project parts
//
//  Created by Dmitry Bystrov.
//  Copyright 2013 E-STUDIO LLC, Inc. All rights reserved.
//------------------------------------------------------------

#include "parts/include.h"
#include "query.h"
#include "query_context.h"
#include "collection.h"
#include "database.h"

namespace parts {
namespace db {

Query::Query(Database* pDatabase, QueryContext* pQueryContext)
  : m_pDatabase(pDatabase),
    m_pQueryContext(pQueryContext) {}

Query::~Query() {}

nE_DataPointer Query::Execute(const nE_Data* pQueryData) {
  nE_DataPointer pResult;

  if (pQueryData->GetType() != nE_Data::Data_Table) {
    m_pQueryContext->GetErrorStorage().Add("A query must be a table.");
  } else {
    const nE_DataTable* pQueryTable = pQueryData->AsTable();

    ParsedQuery parsedQuery(m_pQueryContext);
    if (parsedQuery.Parse(pQueryTable, *m_pDatabase,
                          m_pQueryContext->GetErrorStorage())) {
      if (parsedQuery.m_sQueryType == "find") {
        pResult.reset(Find(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "find_all") {
        pResult.reset(FindAll(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "insert") {
        pResult.reset(Insert(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "update") {
        pResult.reset(Update(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "update_all") {
        pResult.reset(UpdateAll(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "delete") {
        pResult.reset(Delete(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "delete_all") {
        pResult.reset(DeleteAll(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "create") {
        pResult.reset(Create(parsedQuery));
      } else if (parsedQuery.m_sQueryType == "create_if_not_exists") {
        pResult.reset(CreateIfNotExists(parsedQuery));
      } else {
        m_pQueryContext->GetErrorStorage().Add("It is an unknown query.",
                                               parsedQuery.m_sCollectionName.c_str());
      }
    }
  }
  return pResult;
}

bool Query::MayBeQueryTable(const nE_Data* pQueryTable) {
  if (pQueryTable == NULL) {
    return false;
  } else if (pQueryTable->GetType() != nE_Data::Data_Table) {
    return false;
  }
  return (pQueryTable->IsExist("query"));
}

nE_Data* Query::FindResult(const ParsedQuery& parsedQuery,
                           const nE_Data* pCollectionItem) {
  m_pQueryContext->Add(pCollectionItem->AsTable());
  m_pQueryContext->Add(parsedQuery.m_sAlias, pCollectionItem);
  nE_Data* pResult = m_pQueryContext->CalculateValue(parsedQuery.m_pResult,
                     parsedQuery.m_sAlias);
  m_pQueryContext->Remove(parsedQuery.m_sAlias);
  m_pQueryContext->Remove(pCollectionItem->AsTable());
  return pResult;
}

nE_Data* Query::Find(const ParsedQuery& parsedQuery) {
  nE_Data* pResult = NULL;

  nE_DataPointer pResultArray(FindAll(parsedQuery, 1));
  if (pResultArray->GetType() == nE_Data::Data_Array &&
      pResultArray->AsArray()->Size()) {
    pResult = pResultArray->AsArray()->Get(0);
    pResultArray->AsArray()->EraseWithoutDelete(0);
  } else {
    pResult = new nE_Data();
  }
  return pResult;
}

nE_Data* Query::FindAll(const ParsedQuery& parsedQuery, size_t iLimit) {
  ItemVector items;
  FindItems(parsedQuery, iLimit, items);

  nE_DataArray* pResult = new nE_DataArray();
  ItemVector::iterator it = items.begin();
  for (; it != items.end(); ++it) {
    pResult->Push(FindResult(parsedQuery, *it));
  }

  return pResult;
}

nE_Data* Query::Insert(const ParsedQuery& parsedQuery) {
  if (parsedQuery.m_pValue->GetType() == nE_Data::Data_Array) {
    nE_DataArray arrayToInsert = parsedQuery.m_pValue->AsArray();
    for (size_t i = 0; i < arrayToInsert.Size(); i++) {
      nE_DataPointer pResult(m_pQueryContext->CalculateValue(arrayToInsert.Get(
                               i)->AsTable(), parsedQuery.m_sAlias, false));
      parsedQuery.m_pCollection->InsertItem(pResult->AsTable());
    }
  } else {
    nE_DataPointer pResult(m_pQueryContext->CalculateValue(parsedQuery.m_pValue,
                           parsedQuery.m_sAlias, false));
    parsedQuery.m_pCollection->InsertItem(pResult->AsTable());
  }
  SendCollectionUpdated(parsedQuery);
  return new nE_DataInt(1);
}

void Query::UpdateItem(const ParsedQuery& parsedQuery,
                       const nE_Data* pCollectionItem) {
  m_pQueryContext->Add(pCollectionItem->AsTable());
  m_pQueryContext->Add(parsedQuery.m_sAlias, pCollectionItem);
  nE_DataPointer pUpdateSet(m_pQueryContext->CalculateValue(parsedQuery.m_pSet,
                            parsedQuery.m_sAlias, false));
  parsedQuery.m_pCollection->UpdateItem(pCollectionItem->AsTable()->Get(
                                          Collection::DEFAULT_INDEX_NAME), pUpdateSet->AsTable());
  m_pQueryContext->Remove(parsedQuery.m_sAlias);
  m_pQueryContext->Remove(pCollectionItem->AsTable());
}

nE_Data* Query::Update(const ParsedQuery& parsedQuery) {
  return UpdateAll(parsedQuery, 1);
}

nE_Data* Query::UpdateAll(const ParsedQuery& parsedQuery, size_t iLimit) {
  ItemVector items;
  FindItems(parsedQuery, iLimit, items);

  ItemVector::iterator it = items.begin();
  for (; it != items.end(); ++it) {
    UpdateItem(parsedQuery, *it);
  }
  SendCollectionUpdated(parsedQuery);
  return new nE_DataInt((int)items.size());
}

nE_Data* Query::Delete(const ParsedQuery& parsedQuery) {
  return DeleteAll(parsedQuery, 1);
}

nE_Data* Query::DeleteAll(const ParsedQuery& parsedQuery,
                          size_t iLimit /*= INT_MAX */) {
  ItemVector items;
  FindItems(parsedQuery, iLimit, items);

  ItemVector::iterator it = items.begin();
  for (; it != items.end(); ++it) {
    const nE_Data* pCollectionItem = *it;
    parsedQuery.m_pCollection->DeleteItem(pCollectionItem->AsTable()->Get(
                                            Collection::DEFAULT_INDEX_NAME));
  }
  SendCollectionUpdated(parsedQuery);
  return new nE_DataInt((int)items.size());
}

nE_Data* Query::Create(const ParsedQuery& parsedQuery) {
  bool bCreated = true;
  nE_DataTable collectionOptions;
  collectionOptions.Push("name", parsedQuery.m_sCollectionName);
  collectionOptions.PushCopy("indices", parsedQuery.m_pIndices);
  collectionOptions.PushCopy("crypts", parsedQuery.m_pCrypts);
  collectionOptions.PushCopy("items", parsedQuery.m_pItems);
  m_pDatabase->CreateWritableCollection(nE_DataPointer(
                                          collectionOptions.Clone()));
  return new nE_DataBool(bCreated);
}

nE_Data* Query::CreateIfNotExists(const ParsedQuery& parsedQuery) {
  nE_Data* pIsCreated;
  if (m_pDatabase->GetCollection(parsedQuery.m_sCollectionName) ==(CollectionPointer) NULL) {
    pIsCreated = Create(parsedQuery);
  } else {
    pIsCreated = new nE_DataBool(true);
  }
  return pIsCreated;
}

void Query::FindItems(const ParsedQuery& parsedQuery, size_t iLimit,
                      ItemVector& items) {
  const nE_DataTable* pCriteria = parsedQuery.m_pCriteria;
  if (pCriteria == NULL) {
    FindAllAll(parsedQuery.m_pIndex, iLimit, items);
  } else {
    if (pCriteria->IsExist("like")) {
      FindAllLike(parsedQuery.m_pIndex, iLimit, pCriteria->Get("like"), items);
    } else if (pCriteria->IsExist("min") && pCriteria->IsExist("max")) {
      FindAllMinMax(parsedQuery.m_pIndex, iLimit, pCriteria->Get("min"),
                    pCriteria->Get("max"), items);
    } else if (pCriteria->IsExist("exists_in")) {
      FindAllIn(parsedQuery.m_pIndex, iLimit, pCriteria->Get("exists_in"), items);
    } else {
      m_pQueryContext->GetErrorStorage().Add("It is wrong criteria for 'find_all' query.");
    }
  }
}

void Query::FindAllAll(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                       ItemVector& items) {
  CollectionIndex::const_iterator it = pIndex->begin();
  for (; it != pIndex->end() && iLimit > 0; ++it, --iLimit) {
    items.push_back(it->second->AsTable());
  }
}

void Query::FindAllLike(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                        const nE_Data* pLike, ItemVector& items) {
  nE_DataPointer pLikeKey = CollectionIndex::CreateKey(m_pQueryContext->Evaluate(
                              pLike));
  CollectionIndex::const_iterator it = pIndex->find(pLikeKey);
  for (; it != pIndex->end() && iLimit > 0; ++it, --iLimit) {
    if (*pLikeKey == *it->first) {
      items.push_back(it->second->AsTable());
    } else {
      break;
    }
  }
}

void Query::FindAllMinMax(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                          const nE_Data* pMin, const nE_Data* pMax, ItemVector& items) {
  CollectionIndex::const_iterator it = pIndex->lower_bound(
                                         CollectionIndex::CreateKey(m_pQueryContext->Evaluate(pMin)));
  CollectionIndex::const_iterator end = pIndex->upper_bound(
                                          CollectionIndex::CreateKey(m_pQueryContext->Evaluate(pMax)));
  for (; it != end && iLimit > 0; ++it, --iLimit) {
    items.push_back(it->second->AsTable());
  }
}

void Query::FindAllIn(ReadonlyCollectionIndexPointer pIndex, size_t iLimit,
                      nE_Data* pIn, ItemVector& items) {
  nE_DataPointer pTemporaryResult;
  nE_DataArray* pInArray = NULL;
  if (pIn->GetType() == nE_Data::Data_Array) {
    pInArray = pIn->AsArray();
  } else if (pIn->GetType() == nE_Data::Data_String ||
             pIn->GetType() == nE_Data::Data_Table) {
    pTemporaryResult.reset(m_pQueryContext->CalculateValue(pIn, "", false));
    if (pTemporaryResult !=(nE_DataPointer) NULL) {
      pInArray = pTemporaryResult->AsArray();
    }
  }

  if (pInArray != NULL) {
    for (size_t i = 0; i < pInArray->Size() && iLimit > 0; ++i) {
      CollectionIndex::const_iterator it = pIndex->find(CollectionIndex::CreateKey(
                                             pInArray->Get(i)));
      if (it != pIndex->end()) {
        items.push_back(it->second->AsTable());
        --iLimit;
      }
    }
  } else {
    m_pQueryContext->GetErrorStorage().Add("It is wrong criteria 'exists_in'.");
  }
}

void Query::SendCollectionUpdated(const ParsedQuery& parsedQuery) {
  nE_DataTable collectionInfo;
  collectionInfo.Push("collection", parsedQuery.m_sCollectionName);
  nE_Mediator::GetInstance()->SendMessage(Messages::Event_Db_CollectionUpdated,
                                          &collectionInfo);
}

}
}
