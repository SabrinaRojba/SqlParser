public enum StatementType {
//  UNKNOWN means unable to parse, assume storec proc or unsupported statement
    SELECT,
    DELETE,
    INSERT,
    UPDATE,
    EXEC,
    CREATE,
    DROP,
    ALTERTABLE,
    SP,
    UNKNOWN
}