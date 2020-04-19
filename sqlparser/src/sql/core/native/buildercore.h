#ifndef _SQL_BUILDER_CORE_H_
#define _SQL_BUILDER_CORE_H_

#include "sqltable.h"
#include "sqltokenizer.h"

typedef enum _string_case_type {
  SCT_DEFAULT,
  SCT_UPPER_CASE,
  SCT_LOWER_CASE
} STRING_CASE_TYPES;

INT EncodeIdentifier(LPSTR lpszIdentifier, LPSTR lpszOpenQuote, LPSTR lpszCloseQuote, STRING_CASE_TYPES eStringCaseType, DynStr& sEncodedIdentifier) {
  int ret_code;
  if (!lpszIdentifier) {
    if (ret_code = sEncodedIdentifier.Set(lpszIdentifier)) return ret_code;
    return 0;
  }

  if (0 == mystrlen(lpszIdentifier)) {
    if (ret_code = sEncodedIdentifier.Set(lpszIdentifier)) return ret_code;
    return 0;
  }

  if (ret_code = sEncodedIdentifier.Reset()) return ret_code;
  if ((!lpszOpenQuote && 0 == mystrlen(lpszOpenQuote)) || (!lpszCloseQuote && 0 == mystrlen(lpszCloseQuote))) {
    if (ret_code = sEncodedIdentifier.Set(lpszIdentifier)) return ret_code;
  }


  if (!lpszOpenQuote || !lpszCloseQuote) {
    lpszOpenQuote = "[";
    lpszCloseQuote = "]";
  }

  LPSTR lpszSeek = lpszIdentifier;
  for (; *lpszSeek != 0; ++lpszSeek) {
    char ch = *lpszSeek;
    if (ch == *lpszOpenQuote || ch == *lpszCloseQuote) {
      if (ret_code = sEncodedIdentifier.Append("\\")) return ret_code;
    }

    if (ret_code = sEncodedIdentifier.Append(&ch, 1)) return ret_code;
  }

  if (eStringCaseType == SCT_LOWER_CASE) {
    if (ret_code = sEncodedIdentifier.ToLower()) return ret_code;
  }
  else if (eStringCaseType == SCT_UPPER_CASE) {
    if (ret_code = sEncodedIdentifier.ToUpper()) return ret_code;
  }

  return 0;
}

INT QuoteIdentifier(LPSTR lpszIdentifier, LPSTR lpszOpenQuote, LPSTR lpszCloseQuote, DynStr& sQuotedIdentifier) {
  int ret_code;
  if (ret_code = sQuotedIdentifier.Reset()) return ret_code;
  if (0 == mystrcmp("*", lpszIdentifier)) {
    if (ret_code = sQuotedIdentifier.Set(lpszIdentifier)) return ret_code;
    return 0;
  }

  if (!lpszOpenQuote || !lpszCloseQuote) {
    lpszOpenQuote = "[";
    lpszCloseQuote = "]";
  }

  if (ret_code = sQuotedIdentifier.Append(lpszOpenQuote)) return ret_code;
  if (ret_code = sQuotedIdentifier.Append(lpszIdentifier)) return ret_code;
  if (ret_code = sQuotedIdentifier.Append(lpszCloseQuote)) return ret_code;
  return 0;
}

INT BuildTableFullName(SqlTable* lpTable, LPSTR lpszOpenQuote, LPSTR lpszCloseQuote, STRING_CASE_TYPES eStringCaseType, DynStr& sFullName) {
  int ret_code;
  DynStr sEncodeStr, sQuotedStr;
  if (ret_code = sFullName.Reset()) return ret_code;
  if (mystrlen(lpTable->GetCatalog()) > 0) {
    if (ret_code = EncodeIdentifier(lpTable->GetCatalog(), lpszOpenQuote, lpszCloseQuote, eStringCaseType, sEncodeStr)) return ret_code;
    if (ret_code = QuoteIdentifier(sEncodeStr.Deref(), lpszOpenQuote, lpszCloseQuote, sQuotedStr)) return ret_code;
    if (ret_code = sFullName.Append(sQuotedStr.Deref())) return ret_code;
    if (sFullName.Length() > 0) {
      if (ret_code = sFullName.Append(".")) return ret_code;
    }
  }

  if (mystrlen(lpTable->GetSchema()) > 0) {
    if (ret_code = EncodeIdentifier(lpTable->GetSchema(), lpszOpenQuote, lpszCloseQuote, eStringCaseType, sEncodeStr)) return ret_code;
    if (ret_code = QuoteIdentifier(sEncodeStr.Deref(), lpszOpenQuote, lpszCloseQuote, sQuotedStr)) return ret_code;
    if (ret_code = sFullName.Append(sQuotedStr.Deref())) return ret_code;
    if (sFullName.Length() > 0) {
      if (ret_code = sFullName.Append(".")) return ret_code;
    }
  }

  if (mystrlen(lpTable->GetName()) > 0) {
    if (ret_code = EncodeIdentifier(lpTable->GetName(), lpszOpenQuote, lpszCloseQuote, eStringCaseType, sEncodeStr)) return ret_code;
    if (ret_code = QuoteIdentifier(sEncodeStr.Deref(), lpszOpenQuote, lpszCloseQuote, sQuotedStr)) return ret_code;
    if (ret_code = sFullName.Append(sQuotedStr.Deref())) return ret_code;
  }

  return 0;
}

#endif //_SQL_BUILDER_CORE_H_


