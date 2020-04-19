#ifndef _SQL_TABLE_H_
#define _SQL_TABLE_H_

class SqlTable {
  DynStr m_sCatalog, m_sSchema, m_sName, m_sAlias;
public:
  INT Init(LPSTR lpszCatalog, LPSTR lpszSchema, LPSTR lpszName, LPSTR lpszAlias = NULL) {
    int ret_code;
    if (ret_code = m_sCatalog.Set(lpszCatalog)) return ret_code;
    if (ret_code = m_sSchema.Set(lpszSchema)) return ret_code;
    if (ret_code = m_sName.Set(lpszName)) return ret_code;
    if (lpszAlias) {
      if (ret_code = m_sAlias.Set(lpszAlias)) return ret_code;
    }
    else {
      if (ret_code = m_sAlias.Set(lpszName)) return ret_code;
    }
  }

  LPSTR GetCatalog() { return m_sCatalog.Deref(); }
  LPSTR GetSchema() { return m_sSchema.Deref(); }
  LPSTR GetName() { return m_sName.Deref(); }
  LPSTR GetAlias() { return m_sAlias.Deref(); }
};

#endif //_SQL_TABLE_H_


