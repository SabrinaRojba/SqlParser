#ifndef _SQL_PARSER_CORE_H_
#define _SQL_PARSER_CORE_H_

#include "errors.h"
#include "sqltable.h"
#include "sqltokenizer.h"

INT ParsePeriodTableName(SqlTokenizer *lpTokenizer, DynStr& sCatalog, DynStr& sSchema, DynStr& sIdentifier) {
  int ret_code;

  DynStrSeq sPeriodNames;
  SQL_TOKEN *lpToken;
  while (!lpTokenizer->IsEOF()) {
    lpToken = lpTokenizer->NextToken();
    if (lpToken->tkKind == TOKEN_KIND::TK_IDENTIFIER) {
      if (ret_code = sPeriodNames.Append(lpToken->sValue.Deref())) return ret_code;
    }
    else {
      return ERR_SQL_SYNTAX_ERROR;
    }

    lpToken = lpTokenizer->LookaheadToken();
    if (lpToken->Equals(".")) {
      lpTokenizer->NextToken();
      continue;
    }
    else {
      break;
    }
  }

  if (1 == sPeriodNames.Count()) {
    if (ret_code = sCatalog.Reset()) return ret_code;
    if (ret_code = sSchema.Reset()) return ret_code;
    if (ret_code = sIdentifier.Set(sPeriodNames.Deref(0))) return ret_code;
  }
  else if (2 == sPeriodNames.Count()) {
    if (ret_code = sCatalog.Reset()) return ret_code;
    if (ret_code = sSchema.Set(sPeriodNames.Deref(0))) return ret_code;
    if (ret_code = sIdentifier.Set(sPeriodNames.Deref(1))) return ret_code;
  }
  else if (3 == sPeriodNames.Count()) {
    if (ret_code = sCatalog.Set(sPeriodNames.Deref(0))) return ret_code;
    if (ret_code = sSchema.Set(sPeriodNames.Deref(1))) return ret_code;
    if (ret_code = sIdentifier.Set(sPeriodNames.Deref(2))) return ret_code;
  }
  else if (sPeriodNames.Count() > 3) {
    if (ret_code = sCatalog.Set(sPeriodNames.Deref(sPeriodNames.Count() - 1))) return ret_code;
    if (ret_code = sSchema.Set(sPeriodNames.Deref(sPeriodNames.Count() - 2))) return ret_code;
    if (ret_code = sIdentifier.Set(sPeriodNames.Deref(sPeriodNames.Count() - 3))) return ret_code;
  }
  else {
    if (ret_code = sCatalog.Reset()) return ret_code;
    if (ret_code = sSchema.Reset()) return ret_code;
    if (ret_code = sIdentifier.Reset()) return ret_code;
  }

  return 0;
}

#endif //_SQL_PARSER_CORE_H_


