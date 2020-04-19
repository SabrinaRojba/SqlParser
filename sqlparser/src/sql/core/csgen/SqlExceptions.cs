using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus.core;
using RSSBus;


internal class SqlExceptions : LangDictionary {
//cnx
  public const string EF_SQLGEN_STATEMENT = "SQL generation for {0} statements is not supported.";
  public const string EF_COMMANDTREE_WITH_PARAMS = "Error generating SQL for command tree of type {0}\r\n. Exception was: {1}";
  public const string EF_NONPRIMITIVE_TYPE =  "Cannot create parameter of nonprimitive type";
  public const string SCAN_DEFININGQUERY = "Scan Expressions backed by DefiningQuery are not supported.";
  public const string EF_SQLGEN_EXPRESSION = "SQL generation for {0} expressions is not supported.";
  public const string EF_SQLGEN_NEWINSTANCE = "NewInstance expressions without a row type are not supported.";
  public const string LINQ_NO_TABLE_ATTRIBUTE = "{0} is not decorated with the TableAttribute.";
  public const string LINQ_NO_TABLE_FOR_TYPE = "It was not possible to get a table for type {0}";
  public const string LINQ_NULL_QUERY = "Null value for query.";
  public const string EF_UNKNOWN_PRIMITIVE_TYPE = "Unknown primitive type {0}";
  public const string EF_UNSUPPORTED_INFORMATION_TYPE = "The provider does not support the information type {0}.";
  public const string EF_UNSUPPORTED_TYPE = "The underlying provider does not support the type {0}.";
  public const string EF_NO_STORE_TYPE_FOR_EDM_TYPE = "There is no store type corresponding to the EDM type '{0}' of primitive type '{1}'.";
  public const string EF_MANIFEST = "Could not create command: The DbProviderManifest input was null.";
  public const string EF_COMMANDTREE = "Could not create command: The DbCommandTree input was null.";
  public const string EF_SQLGEN_SELECT = "SQL generation is not available for SELECT statements.";
  public const string LINQ_NOT_MARKED_AS_TABLE = "{0} is not marked as a table.";
  public const string LINQ_NULL_EXPRESSION_TREE = "Could not create query: The expression tree was null.";
  public const string LINQ_NO_QUERYABLE_ARGUMENT = "Could not create query: A queryable argument is required.";
  public const string LINQ_NO_MATCHING_ENTITY = "No entity was found meeting the specified criteria.";
  public const string LINQ_UNSUPPORTED_EXPRESSION = "This query expression is not supported.";
  public const string EF_UNSUPPORTED_OPERATOR = "Operator {0} is not supported.";
  public const string LINQ_NO_ITEMS_FOR_PROJECTION = "There are no items for projection in this query.";
  public const string LINQ_NO_MULTIPLE_RESULT_SHAPES = "Multiple result shapes are not supported.";
  public const string LINQ_NOT_QUERYABLE = "Invalid sequence operator call. The type for the operator is not queryable.";
  public const string LINQ_MULTIPLE_WHERE_CLAUSES = "You cannot have more than one where clause in the expression.";
  public const string LINQ_MULTIPLE_SELECT = "You cannot have more than one select statement in the expression.";
  public const string LINQ_MULTITAKE_VALUE = "You cannot have more than 1 Take/First/FirstOrDefault in the expression";
  public const string LINQ_UNARY = "The unary operator {0} is not supported";
  public const string LINQ_BINARY_WHERE = "The where predicate does not support {0} as a binary operator";
  public const string LINQ_WHERE_CLASS = "The member {0} in the where expression cannot be found on the {1} class";
  public const string LINQ_SUBQUERIES = "Subqueries are not supported.";
  public const string LINQ_NONMEMBER = "The member '{0}' is not supported";
  public const string LINQ_NULL_METATABLE = "It was not possible to get metadata for {0}.";
  public const string LINQ_NULL_METAMEMBER = "The member {0} in the select expression cannot be found on the {1} class or is not selectable.";
  public const string LINQ_UNSUPPORTED_MEMBER_TYPE = "The member type is not supported.";
  public const string LINQ_UNSUPPORTED_CONSTANT = "The constant for '{0}' is not supported";
  public const string LINQ_NO_MAPPING = "It was not possible to find a mapping column for {0}";
  public const string LINQ_BAD_STORAGE_PROPERTY = "Bad storage property";
  public const string LINQ_UNHANDLED_BINDING_TYPE = "Unhandled binding type '{0}'";
  public const string EF_SQL_GEN_EXPRESSION = "SQL generation for {0} expressions is not supported.";
  public const string EF_SQLGEN_NEW_INSTANCE = "NewInstance expressions without a row type are not supported.";
  public const string EF_NOT_EXPRESSIONS_UNSUPPORTED = "Not Expressions on complex expressions are not supported.";
  public const string EF_SQLGEN_UNSUPPORTED_RESULT_TYPE = "Nested record types or complex objects are not supported.";
  public const string EF_SQLGEN_NONCONSTANT_VALUE_IN_LIMIT = "Limit expressions are only supported with constant values.";
  public const string LINQ_UNHANDLED_EXPRESSION_TYPE = "Unhandled expression type '{0}'";
  public const string SSRS_COMMAND_TYPE_ONLY = "This connection supports CommandType.Text only.";
  public const string SSRS_TRANSACTIONS = "Transactions not supported";
  public const string SSRS_UNSUPPORTED_IMPERSONATION = "Impersonation is not supported in {0} SSRS Provider for {1}";
  public const string SSRS_INTEGRATED_SECURITY = "Integrated Security is not supported for this provider.";
  public const string SSRS_USERPASS = "Username/Password based authentication is not supported.";
  public const string CANNOT_SET_PROVIDERNAME = "Cannot set ProviderName property";
  public const string NULL_DATAREADER = "DataReader values are null.";
  public const string NULL_DATAREADER_BUFFER = "DataReader buffer is null.";
  public const string HASROWS_NOT_SUPPORTED = "HasRows is not supported at this time.";
  public const string KEYWORD_NOT_SUPPORTED = "Keyword not supported: '{0}'";
  public const string XLS_APPLICATION_SETTINGS = "No application settings for this provider.";
  public const string XLS_TRIAL_INSERT = "Ability to insert rows is not available in the trial version. A full license is required to enable inserts.";
  public const string XLS_TRIAL_DELETE = "Ability to delete rows is not available in the trial version. A full license is required to enable deletes.";
  public const string XLS_TRIAL_UPDATE =  "Ability to update rows is not available in the trial version. A full license is required to enable updates.";
  public const string XLS_CONN_NOT_OPEN = "Connection could not be tested: Connection not open.";
  public const string XLS_INVALID_LICENSE = "The license for this product has already expired or is not valid";
  public const string XLS_REGKEY = "{0} Excel Add-In";
  public const string PROPERTY_MISSING = "{0} property has not been initialized";
  //code
  //XLS
  public const string CACHE_EMPTY_CONNECTION_STRING = "[{0}]: The connection string cannot be empty";
  public const string CACHE_CANNOT_LOAD_DRIVER = "Unable to load the appropriate driver [{0}]. Details: {1}";
  public const string CACHE_CANNOT_CONNECT = "Unable to establish a connection using provider [{0}]. Details: {1}";
  public const string CACHE_DBPROVIDERFACTORIES_XAMARIN = "DbProviderFactories does not exist in Xamarin.";
  public const string CACHE_RANGE = "Value [{0}] does not fall within the expected range,";
  public const string NULL_CONNECTION_STRING = "Connection string cannot be null.";
  public const string CACHE_RETRIEVE_METADATA = "Unable to retrieve the database metadata.";
  public const string NO_PREPARED_STATEMENT = "No prepared statement to execute.";
  public const string JAVADB_NULL = "Commands could not be executed because the Java DB is not open.";
  public const string JAVADB_CLOSED = "Commands could not be executed becauase the Java DB is closed.";
  public const string NO_RESULT_SET = "No result set is currently available.";
  public const string CACHEDRIVER = "No driver for the cache database was configured. Set this with the CacheDriver property.";
  public const string CACHECONNECTION = "The connection string to the cache was not configured. Set this with the CacheConnection property.";
  public const string CACHING_IN_PROGRESS = "Cannot cache until the previous caching operation has completed.";
  public const string CONNECTION_CLOSED = "You must first connect.";
  public const string SCHEMA_MODIFIED = "Unable to cache data. The table schema has been modified.";
  public const string CANNOT_ALTER_SCHEMA = "Unable to alter the table schema. Error: {0}";
  public const string CHANGESET_INACTIVE = "Can only modify cache while in a change set that is active";
  public const string CACHE_PRIMARY_KEYS_REQUIRED = "Primary keys are required to update cached rows.";
  public const string CACHE_PRIMARY_KEYS_NOT_DISCOVERED = "Internal Error: Primary keys were not available.";
  public const string INVALID_DATA_TYPE = "Invalid data type";
  public const string INVALID_COL_DATA_TYPE = "Invalid column data type [{0}].";
  public const string UNSUPPORTED_CULTURE_SETTING = "The culture setting [{0}] is not supported.";
  public const string INVALID_CONNECTION_STRING = "The connection string is invalid.";
  public const string INVALID_CONNECTION_STRING_SYNTAX = "Invalid connection string syntax at index: {0}";
  public const string INVALID_CONNECTION_STRING_KEY = "Invalid connection string key name [{0}]";
  public const string NULL_POINTER_EXCEPTION = "Null pointer exception";
  public const string NOT_A_BOOLEAN = "{0} is not a boolean value";
  public const string RESULTSET_NOT_READY = "Result set is not ready.";
  public const string INVALID_OUTPUT_PARAMETER = "Parameter at index [{0}] is not an output parameter.";
  public const string NO_PARAMETER_FOUND = "No parameter named [{0}] was found.";
  public const string UNSUPPORTED_FEATURE = "Feature is not supported.";
  public const string BAD_NUMBER_FORMAT = "Bad number format: [{0}].";
  public const string MAX_PARAMETERS_EXCEEDED = "Procedure may have at most ({0}) parameters.";
  public const string METADATA_INDEX_OUT_OF_RANGE = "Index [{0}] in the parameter metadata is out of range.";
  public const string READONLY_DATASOURCE = "The read-only datasource supports only SELECT queries.";
  public const string CREATE_TABLE = "The query cannot be executed: [CREATE TABLE] is not supported.";
  public const string TABLE_ALREADY_EXISTS = "The table [{0}] already exists.";
  public const string DROP_TABLE_UNSUPPORTED = "The query cannot be executed: [DROP TABLE] is not supported.";
  public const string SQLPARSER_NULL = "Invalid statement for the command type specified.";
  public const string CANNOT_BIND_PARAMETER = "Unable to bind the parameter: {0}.";
  public const string UPDATE_READONLY_COLUMN = "Column [{0}] could not be updated. This column is read-only.";
  public const string NUMERIC_COL_DATATYPE_INVALID = "Invalid schema configuration: The scale and size (precision) must be numeric.";
  public const string MAX_PRECISION_REQUIRES_INT = "Incorrect value for the connection property Max Precision. It requires an integer. The following value was provided: {0}.";
  public const string EXECUTE_FAILED = "Could not execute the specified command: {0}";
  public const string IGNORETYPES_INVALID = "Incorrect value for the connection property Ignore Types. It must be a comma-separated string of types. The following value was provided: [{0}].";
  public const string LOAD_RUNTIME_SETTINGS_FAILED = "Loading runtime settings failed. The file specified does not exist. File: {0}.";
  public const string LOCATION_DOES_NOT_EXIST = "The file or the folder provided in the connection string does not exist. Location: {0}";
  public const string NO_TEST_CONNECTION = "This provider does not support testing connections.";
  public const string INVALID_OBJECT_NAME = "Invalid object name '{0}'.";
  public const string INVALID_COLUMN_NAME = "Invalid column name '{0}'.";
  public const string CANNOT_GET_INFO_ITEM = "Unable to get the info item.";
  public const string INVALID_METHOD = "Invalid method {0} found in the script.";
  public const string CANNOT_RETRIEVE_COLUMNS = "Unable to retrieve columns for table [{0}].";
  public const string REFERENCE_MISSING_TABLE_AND_COLUMN = "Reference [{0}] does not contain table name and column name.";
  public const string NO_CACHE_CONNECTION = "The Cache Connection property was not set.";
  public const string CONNECTION_QUERYCACHE_INT = "The QueryCache property requires a numeric value.";
  public const string CONNECTION_SCHEMACACHEDURATION_INT = "The SchemaCacheDuration property requires an integer value.";
  public const string MAXCONNECTIONS_INT = "The MaxConnections property requires an integer value.";
  public const string CACHE_DB_TYPE_UNRECOGNIZED = "The value for Cache DB Type was not recognized: [{0}]. Possible values are 'MySQL', 'SQLServer', 'Oracle', and 'Generic'.";
  public const string CREATE_TEMPDB = "Temp database creation failed. Error: {0}";
  public const string CLOSE_TEMPDB = "Temp DB failed to close. Error: {0}";
  public const string EF_DESIGN_TIME_METADATA = "The metadata used for additional design-time integration could not be loaded.";
  public const string INDEX_OUT_OF_RANGE = "Index out of range: The index must be between 1 and {0}.";
  public const string QUERYCACHE_WITH_NONSQLITE_DRIVER = "Unable to establish a connection. The following drivers are supported with QueryCache: [{0}].";
  public const string PARAM1_CANNOT_BE_FOUND_IN_METADATADB = "The table '{0}' was not found in the metadata database.";
  public const string ONE_READMODE = "Only one value can be specified. Possible values are ReadAsNull and ReadAsEmpty.";
  public const string INVALID_GETUPDATEBITS = "Only one value can be specified. Possible values are UpdateToNull, UpdateToEmpty, and IgnoreInUpdate.";
  public const string INVALID_GETINSERTBITS = "Only one value can be specified. Possible values are InsertAsNull, InsertAsEmpty, and IgnoreInInsert.";
  public const string CONNECTION_MODE_INVALID = "Unknown mode encountered: [{0}]";
  public const string CACHEPARTIAL_FALSE = "The AutoCache property can only be used for queries that cache all columns (not a projection). To cache partially, set CachePartial=True in addition to AutoCache.";
  public const string UNABLE_TO_LOAD_PARAM1 = "Unable to load the requested data. Detail: {0}";
  public const string EXEC_CACHE_NULL = "Cannot execute command: Cache has not been configured.";
  public const string INVALID_QUERY = "Invalid query [{0}].";
  public const string UNSUPPORTED_CACHE_QUERY = "Unsupported cache query: {0}";
  public const string MISSING_TRANSACTION_ID = "Transaction Id was missing.";
  public const string TRANSACTION_ALREADY_OPEN = "Transaction [{0}] is already open.";
  public const string TRANSACTION_DOES_NOT_EXIST = "The transaction [{0}] does not exist.";
  public const string MULTIPLE_SYNC_BLOCKS = "Multiple sync blocks were detected but EnableMultipleSyncs was set to false.";
  public const string TRANSACTION_NOT_SUPPORTED = "Transaction is not supported.";
  public const string BULKOPERATION_NOT_SUPPORTED = "Bulk operations are not supported.";
  public const string MIXBULKOPERATION_NOT_SUPPORTED = "Mixing bulk operation types is not supported.";
  public const string UPDATEGRAMS_ERROR = "Updategrams error: {0}";
  public const string UPDATE_VIEW_FAILED = "Views cannot be updated.";
  public const string TEMPDB_CREATE_TABLE = "Unable to create the table in the temp database. Detail: {0}";
  public const string NO_SCHEMA = "Unable to get schema.";
  public const string NONTEXTCOMMAND_IN_CACHE = "Nontext commands cannot be executed in the cache.";
  public const string UNKNOWN_FUNCTION = "Unknown function: {0}";
  public const string UNKNOWN_DATATYPE = "Unknown datatype: {0}.";
  public const string INVALID_COLUMN_INDEX = "Invalid column index [{0}]";
  public const string TABLE_COLUMNS = "Unable to get table columns. {0}";
  public const string TABLE_METADATA = "Unable to get table metadata. Error: {0}";
  public const string GETARRAY_INT = "getArray(int i) is not supported.";
  public const string GETARRAY_STRING = "getArray(String colName) is not supported.";
  public const string GETASCIISTREAM_INT = "getAsciiStream(int columnIndex) is not supported.";
  public const string GETASCIISTREAM_STRING = "getAsciiStream(String columnName) is not supported.";
  public const string RESULTSET_CURSOR = "You must iterate through the result set and check for the next row before reading from it.";
  public const string GETBINARYSTREAM_INT = "getBinaryStream(int columnIndex) is not supported.";
  public const string GETBINARYSTREAM_STRING = "getBinaryStream(String columnName) is not supported.";
  public const string GETBLOB_INT = "getBlob(int i) is not supported.";
  public const string GETBLOB_STRING = "getBlob(String colName) is not supported.";
  public const string GETCHARACTERSTREAM_INT = "getCharacterStream(int columnIndex) is not supported.";
  public const string GETCHARACTERSTREAM_STRING = "getCharacterStream(String columnName) is not supported.";
  public const string GETCLOB_INT = "getClob(int i) is not supported.";
  public const string GETCLOB_STRING = "getClob(String colName) is not supported.";
  public const string GETFETCHDIRECTION = "getFetchDirection() is not supported.";
  public const string GETFETCHSIZE = "getFetchSize() is not supported.";
  public const string GETOBJECT_INT = "getObject(int i, Map<String, Class<?>> map) is not supported.";
  public const string GETOBJECT_STRING = "getObject(String colName, Map<String, Class<?>> map) is not supported.";
  public const string GETREF_INT = "getRef(int i) is not supported.";
  public const string GETREF_STRING = "getRef(String colName) is not supported.";
  public const string GETUNICODESTREAM_INT = "getUnicodeStream(int columnIndex) is not supported.";
  public const string GETUNICODESTREAM_STRING = "getUnicodeStream(String columnName) is not supported."; 
  public const string GETURL_INT = "getURL(int columnIndex) is not supported.";
  public const string GETURL_STRING = "getURL(String columnName) is not supported.";
  public const string METADATA_TABLES_READONLY = "Only SELECT queries can be executed on metadata tables.";
  public const string PARAMETER_NOT_FOUND = "Parameter {0} was not found in the parameter collection.";
  public const string UNRECOGNIZED_COLUMN = "Unrecognized column [{0}] in query for table [{1}].";
  public const string OAUTHSETTINGS_READ = "An error was encountered when reading the OAuthSettings: [{0}]";
  public const string OAUTHSETTINGS_WRITE = "An error was encountered when writing the OAuthSettings to disk: [{0}]";  
  public const string INITIATEOAUTH = "'OAuthClientId' and 'OAuthClientSecret' are needed to initiate OAuth.";
  public const string INVALID_STATEMENT = "Invalid statement.";
  public const string ARRAY_PARAM = "Array type parameters are not currently supported";
  public const string ASCIISTREAM = "ASCII stream parameters are not currently supported";
  public const string BIGDECIMAL = "Big Decimal type parameters are not currently supported";
  public const string BINARYSTREAM = "Binary stream parameters are not currently supported";
  public const string BLOB = "Blob type parameters are not currently supported";
  public const string CHARACTERSTREAM = "Character stream parameters are not currently supported";
  public const string CLOB = "Clob type parameters are not currently supported";
  public const string REF_PARAM = "Ref type parameters are not currently supported.";
  public const string UNICODESTREAM = "Unicode stream parameters are not currently supported";
  public const string CONNECTION_OPEN =  "Unable to open the connection [{0}].";
  public const string PARSE_INT = "Error parsing int value [{0}].";
  public const string NULL_ANALYZER = "Analyzer name is NULL.";
  public const string UNSUPPORTED_ANALYZER = "Analyzer '{0}' is not supported";
  public const string AT_LEAST_ONE_PARAM = "At least one parameter is required.";
  public const string TOO_MANY_OPEN_CONNECTIONS = "Too many open connections. You have configured the provider to have no more than {0} connections open at a time. Ensure that connections are closed after use.";
  public const string REQUIRES_THREE_PARAMS = "You must set three parameters.";
  public const string INVALID_DATETIME_WITH_PARAMS = "The value is not a valid datetime. Provided [{0}].";
  public const string YEAR_UNSUPPORTED = "Year is not yet supported.";
  public const string QUARTER_UNSUPPORTED = "Quarter is not yet supported.";
  public const string MONTH_UNSUPPORTED = "Month is not yet supported.";
  public const string DAYOFYEAR_UNSUPPORTED = "Dayofyear is not yet supported.";
  public const string WEEK_UNSUPPORTED = "Week is not yet supported.";
  public const string WEEKDAY_UNSUPPORTED = "Weekday is not yet supported.";
  public const string MICROSECOND_UNSUPPORTED = "Microsecond is not yet supported.";
  public const string NANOSECOND_UNSUPPORTED =  "Nanosecond is not yet supported.";
  public const string INVALID_DATEPART_WITH_PARAMS = "Invalid date part string [{0}].";
  public const string INVALID_YEAR = "Year is not a valid value.";
  public const string INVALID_MONTH = "Month is not a valid value.";
  public const string INVALID_DAY = "Day is not a valid value.";
  public const string TZOFFSET_UNSUPPORTED = "TZoffset is not yet supported.";
  public const string REQUIRES_TWO_PARAMS = "Two parameters are required.";
  public const string INVALID_DATETIME = "Invalid date-time string.";
  public const string INVALID_DATEPART = "Invalid date-part string.";
  public const string NULL_DATEPART = "The date part cannot be null.";
  public const string AT_LEAST_TWO_PARAMS = "You must set at least two parameters.";
  public const string AT_MOST_THREE_PARAMS = "You can set at most three parameters.";
  public const string DATEFIRST_INT_RANGE = "The datefirst parameter must be an integer from 1 to 7.";
  public const string INVALID_PRECISION = "Precision parameter is not valid.";
  public const string INVALID_FRACTION = "Fraction is not valid.";
  public const string INVALID_SECONDS = "Seconds is not a valid value.";
  public const string INVALID_MINUTE = "Minutes is not a valid value.";
  public const string INVALID_HOUR = "Hour is not a valid value.";
  public const string REQUIRES_EIGHT_PARAMETERS = "Eight parameters are required.";
  public const string INVALID_MILLISECONDS = "Milliseconds are not a valid value.";
  public const string REQUIRES_SEVEN_PARAMETERS = "You must set seven parameters.";
  public const string MONTH_TO_ADD_INT = "The value for month_to_add must be an integer.";
  public const string INVALID_START_DATE = "The value for start_date is not a valid date.";
  public const string AT_MOST_TWO_PARAMS = "You must set two parameters.";
  public const string POSITIVE_COUNT = "The count value requires a positive number.";
  public const string CAST_TO_FLOAT = "Unable to cast [{0}] to a float.";
  public const string CAST_TO_INT = "Unable to cast [{0}] to an integer.";
  public const string CAST_TO_NUM = "Unable to cast [{0}] to number.";
  public const string REQUIRES_POSITIVE = "This argument requires a positive number. Provided: [{0}].";
  public const string NOARGS = "No parameters are accepted.";
  public const string POSITIVE_REPEAT = "You must provide a positive number for the repeat count.";
  public const string POSITIVE_CHAR_COUNT = "You must provide a positive number for the characters to count.";
  public const string REQUIRES_FIVE_PARAMS = "You must set five parameters.";
  public const string POSITIVE_DECIMAL = "You must provide a positive number for the decimal parameter.";
  public const string POSITIVE_LEN = "You must specify a positive number for the length.";
  public const string CONVERT = "You must provide a number to convert.";
  public const string START_INT = "You must specify an integer for the start position.";
  public const string LENGTH_INT = "You must specify an integer for the length.";
  public const string NO_SUCH_TABLE = "No such table [{0}].";
  public const string UNRECOGNIZED_KEYWORD = "Unrecognized keyword: {0}";
  public const string ODBC_CANNOT_CONNECT = "Cannot connect to the server: invalid configuration.";
  public const string CANNOT_OPEN_HELP = "The help file cannot be opened.";
  public const string CANNOT_OPEN_HELP_WITH_PARAMS = "The help file '{0}' cannot be opened.";
  public const string INVALID_DSN = "The Data Source Name '{0}' is invalid. ODBC Setup";
  public const string NATIVE_CODE = "All functions should be returned in native code.";
  public const string NO_PROVIDER = "There is not a provider named [{0}].";
  public const string NO_UPDATE = "Input an update statement.";
  public const string NO_INSERT = "Input an insert statement.";
  public const string NO_DELETE = "Input a delete statement.";
  public const string NO_SELECT = "Input a select statement.";
  public const string ARRAY_ARG = "Argument must be an array.";
  public const string EXPECTED_COMMA = "Expected ',', found '{0}'";
  public const string UNEXPECTED_DELIMITER = "Unexpected delimiter '{0}'.";
  public const string EXPECTED_SCALE = "Syntax error near [{0}]. Expected scale.";
  public const string EXPECTED_LEN = "Syntax error near [{0}]. Expected length.";
  public const string EXPECTED_DATATYPE = "Syntax error near [{0}]. Expected datatype.";
  public const string EXPECTED_END_PAREN_FOUND_PARAM1 = "Expected end parenthesis. Found {0}.";
  public const string UNEXPECTED_END_BEFORE_END_PAREN = "Unexpected end of statement. Expected end parenthesis.";
  public const string UNEXPECTED_END_BEFORE_PARAM = "Unexpected end of statement. Expected a parameter.";
  public const string UNEXPECTED_END = "Unexpected end of statement while parsing criteria.";
  public const string EXPECTED_COLNAME_OR_PARAM1_FOUND_PARAM2 = "Expected column name, ',', or '{0}'. Found '{1}'.";
  public const string PAREN_LIST_FOLLOWS_PARAM1 = "The [{0}] must be followed by a list of items enclosed in parentheses.";
  public const string OPERATOR_CANNOT_BE_NEGATED = "Unable to negate the operator {0}";
  public const string UNMATCHED_OPENING_PAREN = "Found '(' with no matching ')'.";
  public const string UNMATCHED_CLOSING_PAREN = "Found ')' with no matching '('.";
  public const string UNEXPECTED_END_AFTER_WHERE = "Expected criteria after 'WHERE', but statement ended prematurely.";
  public const string EXPECTED_WHERE_FOUND_PARAM1 = "Expected WHERE. Found {0}";
  public const string INSERT_ROW = "The row could not be inserted.";
  public const string UPDATE_ROW = "The row could not be updated.";
  public const string DELETE_ROW = "The row could not be deleted.";
  public const string ENDED_BEFORE_COLNAME_FOUND = "Expected column name after table name, but statement ended prematurely.";
  public const string ENDED_BEFORE_COLNAME_PARSED = "Unexpected end of statement while parsing column name.";
  public const string EXPECTED_GROUP = "Expected GROUP. Found: {0}";
  public const string EXPECTED_HAVING = "Expected HAVING. Found: {0}";
  public const string EXPECTED_NUM_FOUND_PARAM1 = "Expected a number or ','. Found '{0}' instead.";
  public const string EXPECTED_PARAM1_FOUND_PARAM2 = "Expected {0}. Found: {1}";
  public const string EXPECTED_COLNAME_FOUND_PARAM1 = "Expected a column name or ','. Found '{0}' instead.";
  public const string EXPECTED_ORDER = "Expected ORDER. Found: {0}";
  public const string EXPECTED_FIRST_LAST_AFTER_NULLS = "Expected FIRST or LAST after NULLS";
  public const string MISSING_CLOSING_PAREN = "Missing closing parenthesis.";
  public const string SYNTAX = "Syntax error near [{0}]";
  public const string EXPECTED_FROM_FOUND_PARAM1 = "Expected FROM. Found: {0}";
  public const string PARSE_DOUBLE = "Error parsing double value [{0}].";
  public const string INVALID_INT = "The value {0} is not a valid integer.";
  public const string EMPTY_STATEMENT = "Empty statement.";
  public const string EXPECTED_CLOSING_PAREN = "Expected ')' but statement ended prematurely.";
  public const string VALUE_CLAUSE_UNMATCHED = "VALUE clause has more values than the column list.";
  public const string ENDED_BEFORE_EXPECTED_VALUE = "Expected value but statement ended prematurely.";
  public const string EXPECTED_VALUES = "Expected 'VALUES'. Found: {0}";
  public const string EXPECTED_START_PAREN = "Expected '('. Found: {0}";
  public const string INSERT_SELECT_STAR = "Column name '*' cannot be used for INSERT-SELECT query.";
  public const string EXPECTED_TABLENAME_BEFORE_WHERE = "Expected table name before WHERE keyword.";
  public const string EXPECTED_EQUALS = "Expected '=' but found '{0}'.";
  public const string ENDED_BEFORE_PARAMNAME_VALUE = "Expected parameter=value expression but statement ended prematurely.";
  public const string ENDED_BEFORE_TABLENAME = "Expected table name but statement ended prematurely.";
  public const string ENDED_BEFORE_DATATYPE = "Expected data type but statement ended prematurely.";
  public const string ENDED_BEFORE_COLSIZE = "Expecting column size but statement ended prematurely.";
  public const string ENDED_BEFORE_COLNAME = "Expecting column name but statement ended prematurely.";
  public const string NO_SELECT_WITH_EXECUTEUPDATE = "executeUpdate does not accept SELECT statements. Use executeQuery instead.";
  public const string USE_EXECUTEQUERY_WITH_SP = "Use executeQuery to execute stored procedures.";
  public const string EXECUTEQUERY_ACCEPTS_SELECT_ONLY = "executeQuery only accepts the SELECT statement. Use executeUpdate instead.";
  public const string ENDED_BEFORE_COLNAME_VALUE = "Expected column=value expression but statement ended prematurely.";
  public const string ENDED_BEFORE_PARAM1 = "Expected {0} but statement ended prematurely.";
  public const string EXPECTED_IDENTIFIER = "Expected identifier but found {0} with value '{1}' instead at position {2}.";
  public const string EXPECTED_PRIMARY_KEY = "Expected primary key but statement ended prematurely.";
  public const string UPDATE_SELECT_STAR = "Column name '*' cannot be used for UPDATE-SELECT query.";
  public const string SQL_COLCOUNT = "SQL column count does not match.";
  public const string MALFORMED_SQL = "Malformed SQL Statement: {0}\r\nStatement:{1}";
  public const string INPUT_READ_ERROR = "End of input text was reached.";
  public const string EXPECTED_ENDQUOTE = "Could not find expected closing quote character {0} matching the one found at position {1}";
  public const string UNKNOWN_TOKEN = "Unknown token {0} encountered during parsing at position {1}";
  public const string EXPECTED_TOKEN = "Expected token '{0}' but found {1} with value '{2}' instead at position {3}.";
  public const string NO_COL_DEF = "No column definition [{0}].";
  public const string ENDED_BEFORE_COL_DEF = "Expected column definition but statement ended prematurely.";
  public const string SYNTAX_INT = "Syntax error near [{0}]. Expected an integer value.";
  public const string UNSUPPORTED_CONSTRAINT = "Constraint is not supported yet. Only the primary key can be used.";
  public const string UNEXPECTED_TOKEN = "Unexpected token encountered: [{0}].";
  public const string INVALID_DATA_TYPE_WITH_PARAMS = "The value [{0}] is not valid as a data type.";
  public const string EXPECTED_ANON_PARAM = "Expected '?' but found: {0}.";
  public const string ENDED_BEFORE_CLOSING_PAREN = "Statement ended prematurely before ')'.";
  public const string ENDED_BEFORE_QUERY = "Expected a query but the statement ended prematurely.";
  public const string ENDED_BEFORE_SP_NAME = "Expected stored procedure name but statement ended prematurely.";
  public const string EXPECTED_SELECT_OR_GETDELETED = "Expected next token as SELECT or GETDELETED but found [{0}]";
  public const string REQUIRES_INT_PARAM = "You must provide an integer. Provided [{0}].";
  public const string REQUIRES_ONE_PARAM = "You must set one parameter.";
  public const string CREATESCHEMA = "No procedure to create the schema was found.";
  public const string NEW_CHANGESET = "Cannot begin a new changeset before completing the previous one.";
  public const string CONNECTION_STRING_MODIFY_OPEN = "The connection string cannot be modified after the connection has been opened.";
  public const string SELECT_STAR = "'*' must be the only column in the query when used.";
  public const string COLUMN_NOT_FOUND = "The column '{0}' was not found.";
  public const string COLUMN_DOES_NOT_EXIST = "The column [{0}] does not exist";
  public const string INVALID_STRING_OPERATOR = "The operator '{0}' cannot be used on string columns. Column: [{1}].";
  public const string CONNECTION_SUCCESSFUL = "The connection test was successful.";
  public const string JAVA_PARAMETER_EXCEPTION = "Invalid parameter index: {0}. Index must be greater than 0 and no more than the number of parameters specified in the query.";
  public const string INVALID_CACHE_OPTION = "Invalid cache option {0}. This is only available with REPLICATE.";
  public const string NO_SUPPORT_OAUTH_NOPROMPT = "OAuth authentication is not supported.";
  public const string CURSOR_MOVE_PRIVIOUS_UNSUPPORTED = "Moving previous is not supported by current cursor.";
  public const string NOT_FOUND_DIALECT_SQL_BUILDER = "The '{0}' Dialect SqlBuilder can't be found.";
  public const string NO_SUPPORT_SQL_NORMALIZATION = "Normalization is not supported for the SQL statement '{0}'.";
  public const string NO_TABLE_REFERENCE_INCLUDE_IN_FROM = "No table found with the alias '{0}'.";
  public const string NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR = "Deleting the left or right expression of SqlCondition is not supported.";
  public const string NO_SUPPORT_EQUAL_IN_DATELITERALS = "Equals is not supported in date literals.";
  public const string INVALID_PARAMETER_IN_LITERAL = "Invalid parameter in literal '{0}'";
  public const string UNKNOWN_COLUMN_IN_FIELDS_LIST = "Unknown column '{0}'.";
  public const string AMBIGUOUS_COLUMN_IN_FIELDS_LIST = "Column '{0}' is ambiguous.";
  public const string INVALID_TYPE_FORMATTER = "Invalid data type formatter.";
  public const string CROSS_APPLY_UNKNOWN_TABLE = "Column reference in CROSS APPLY clause references unknown table with prefix '{0}'.";
  public const string CROSS_APPLY_INVALID_TVF = "CROSS APPLY clause is followed by unsupported table-valued function '{0}'.";
  public const string CROSS_APPLY_NO_COLUMNS = "Table-valued function requires '{0}' at least 1 column definition in the WITH clause.";
  public const string CROSS_APPLY_NO_COLUMN_REFERENCE = "Table-valued function '{0}' requires a column reference as the first argument.";
  public const string CROSS_APPLY_INVALID_EXPRESSION = "Argument '{0}' to table-valued function '{1}' must be a constant expression.";
  public const string CROSS_APPLY_DELIMITED_TABLE_TOO_MANY_COLUMNS = "DELIMITEDTABLE expression only supports a single column definition.";
  public const string WARN_STRING = "Warning: {0}";

  public static SqlExceptions INSTANCE = new SqlExceptions();
  private JavaHashtable<string,string> j = null;
  internal protected  SqlExceptions() {
    j = new JavaHashtable<string,string>();
    // License Exceptions
    j.Put("This system contains a license for {0} that has been installed but not activated.  You must run setup in order to activate the license on this system [code: {3} nodeid: {4}].",
    "このシステム上には、{0}用のライセンスがインストールされていますが、ライセンス認証されていない状況です。ライセンス認証を行うには、セットアップを実行する必要があります [コードcode：{3} nodeid：{4}]。");
    j.Put("This system contains a trial license for {0} that has expired.  Please visit {1} or email {2} for information on purchasing a license or extending your trial [code: {3} nodeid: {4}].",
    "このシステム上には、有効期間が切れている{0}用の試用ライセンスがインストールされています。製品ライセンスの購入、または試用ライセンスの有効期間を延長するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。");
    j.Put("Could not find a valid license for using {0} on this system.  To obtain a trial license, please visit {1} or email {2} [code: {3} nodeid: {4}].",
    "このシステム上に{0}を使用するための有効なライセンスが見つかりませんでした。試用ライセンスを入手するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。");
    j.Put("Error reading registry: {0}",
    "レジストリを読み込み中にエラーが発生しました。{0}");
    j.Put("Could not find a valid license for using {0} on this system.  To obtain a trial license, please visit {1} or email {2} [code: {3} nodeid: {4}].If you believe you already have a license, please check the following path(s):{5}, {6}.",
    "このシステム上に{0}を使用するための有効なライセンスが見つかりませんでした。試用ライセンスを入手するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。既にライセンスを入手している場合は、{5}、{6}のパスをご確認ください。");
    j.Put("Invalid product key [code: {0} nodeid: {1}].",
    "無効なプロダクトキー[コード：{0} nodeid：{1}]。");   
    j.Put("You must enter a valid Product Key in order to continue.",
    "続行するには、有効なプロダクトキーを入力する必要があります。"); 
    j.Put("Contact {0} for further help.",
    "詳細は{0}までお問い合わせください。");
    j.Put("Error downloading license: {0}",
    "ライセンスをダウンロード中にエラーが発生しました。{0}");
    j.Put("\r\nPlease try to activate manually by going to:",
    "\r\n手動でライセンス認証を行う："); 
    j.Put("Could not find {0}.jdbc.{1}.jar.",
    "{0}.jdbc.{1}.jarが見つかりません。");
    j.Put("Error validating user input: {0}",
    "ユーザー入力の検証中にエラーが発生しました。{0}");
    j.Put("License verification failed [code: {0}]. Please try again.",
    "ライセンスの検証に失敗しました [コード：{0}]。ライセンス認証を再度行ってください。");
    j.Put("The license check failed for the following reason: {0}, code: {1}",
    "ライセンス検証失敗の原因：{0}、コード：{1}"); 
    j.Put("Failed to get system version information. [code: {0} nodeid: {1}]",
    "システムバージョン情報の取得に失敗しました。[コード：{0} nodeid：{1}]"); 
    j.Put("This application uses an evaluation version of the {0}. You may test it for a period of 30 days as specified in the Licensing Agreement. Further use requires a license. For more information, please visit {2}.",
    "{0}({1} component)。\r\nこのアプリケーションは、コンポーネントの評価版を使用しています。ライセンス使用許諾契約書により、30日間を通じて評価版を使用できます。ご使用を続行したい場合、製品ライセンスをご購入ください。詳細については、{2}をご覧ください。");
    j.Put("This license can not be used in a Server Operating System [code: {1} nodeid: {2}]",
    "このライセンスは、サーバー・オペレーティング・システム上で使用できません[コード：{1} nodeid：{2}]");
    j.Put("The beta version of {0} used by this application has expired.\nPlease visit {1} for an updated version.",
    "このアプリケーションで使用している{0}のベータ版の有効期間が切れました。\n更新版については、{1}をご覧ください。");
    j.Put("License installation succeeded.",
    "ライセンスのインストールは成功しました。");
   // Core Provider Exceptions
    j.Put("Incorrect value for the connection property Max Precision. It requires an integer. The following value was provided: {0}.",
    "接続プロパティの最大有効桁数の値が不正確です。整数値が必須です。設定された値は{0}です。");
    j.Put("Could not execute the specified command: {0}",
    "指定したコマンドが実行できませんでした。{0}");

    //ado
    j.Put(EF_SQLGEN_STATEMENT,
    "{0}ステートメント用のSQL生成はまだサポートされていません。");
    j.Put(EF_COMMANDTREE_WITH_PARAMS,
    "型{0}のコマンドツリー用のSQLを生成中にエラーが発生しました。\r\n 例外：{1}");
    j.Put(EF_NONPRIMITIVE_TYPE,
    "プリミティブ型以外のパラメータを作成できません。");
    j.Put(EF_SQLGEN_EXPRESSION,
    "{0}式用のSQL生成はまだサポートされていません。");
    j.Put(SCAN_DEFININGQUERY,
    "DefiningQueryに対応されたScan式はサポートされていません。");
    j.Put(EF_SQLGEN_NEW_INSTANCE,
    "行型のないNewInstance式はサポートされていません。");
    j.Put(EF_SQLGEN_UNSUPPORTED_RESULT_TYPE,
    "入れ子になったレコード型または複合型オブジェクトはサポートされていません。");
    j.Put(EF_SQLGEN_NONCONSTANT_VALUE_IN_LIMIT,
    "Limit式は定数値のみでサポートされています。");
    j.Put(LINQ_NO_TABLE_ATTRIBUTE,
    "{0}はTableAttributeで修飾されていません。");
    j.Put(LINQ_NO_TABLE_FOR_TYPE,
    "型{0}のテーブルを取得できませんでした。");
    j.Put(EF_UNKNOWN_PRIMITIVE_TYPE,
    "不明なPrimitiveTypeKind {0}");
    j.Put(EF_UNSUPPORTED_INFORMATION_TYPE,
    "プロバイダは、informationType'{0}'をサポートしていません。");
    j.Put(EF_UNSUPPORTED_TYPE,
    "プロバイダは、type'{0}'をサポートしていません。");
    j.Put(EF_NO_STORE_TYPE_FOR_EDM_TYPE,
    "プリミティブ型'{1}'のEDM型'{0}'に対応するストア型はありません。");
    j.Put(EF_MANIFEST,
    "コマンドが作成できませんでした：DbProviderManifest の入力値は nullです。");
    j.Put(EF_SQLGEN_SELECT,
    "SELECTステートメント用のSQL生成は許可されていません。");  
    j.Put(EF_COMMANDTREE,
    "コマンドが作成できませんでした：DbCommandTree の入力値は nullです。");
    j.Put(LINQ_NOT_MARKED_AS_TABLE,
    "{0}がTableとしてマークされていません。");
    j.Put(LINQ_NULL_EXPRESSION_TREE,
    "クエリが作成できませんでした：式ツリーの値は null です。");
    j.Put(LINQ_NO_QUERYABLE_ARGUMENT,
    "クエリが作成できませんでした：クエリ可能な引数が必要です。"); 
    j.Put(LINQ_NO_MATCHING_ENTITY,
    "指定された基準に一致するエンティティはありません。");
    j.Put(LINQ_UNSUPPORTED_EXPRESSION,
    "このクエリ式はサポートされていません。");
    j.Put(EF_UNSUPPORTED_OPERATOR,
    "演算子{0}はサポートされていません。");

   
    //linq
    j.Put(LINQ_NO_ITEMS_FOR_PROJECTION, 
    "このクエリには、射影するアイテムが含まれていません！");
    j.Put(LINQ_NO_MULTIPLE_RESULT_SHAPES,
    "複数の結果形状がサポートされていません。");
    j.Put(LINQ_NOT_QUERYABLE,
    "無効なシーケンス演算子の呼び出しです。演算子の型はクエリ可能ではありません！");
    j.Put(LINQ_MULTIPLE_WHERE_CLAUSES,
    "式には、2つ以上のWhere句を使用することができません。");
    j.Put(LINQ_MULTIPLE_SELECT,
    "式には、2つ以上のSelectステートメントを使用することができません。");
    j.Put(LINQ_MULTITAKE_VALUE,
    "式には、2つ以上のTake/First/FirstOrDefaultを使用することができません。");
    j.Put(LINQ_UNARY,
    "単項演算子'{0}'がサポートされていません。");
    j.Put(LINQ_BINARY_WHERE,
    "Where述語は'{0}'二項演算子をサポートしません。");
    j.Put(LINQ_WHERE_CLASS,
    "Where式内のメンバー{0}は、{1}クラス上に見つかりません。");
    j.Put(LINQ_SUBQUERIES,
    "サブクエリがサポートされていません。");
    j.Put(LINQ_NONMEMBER,
    "メンバー'{0}'がサポートされていません。");
    j.Put(LINQ_NULL_METATABLE,
    "{0}のメタデータを取得できませんでした。");
    j.Put(LINQ_NULL_METAMEMBER,
    "Select式内のメンバー{0}は、{1}クラス上に見つかりません。または選択可能ではありません。");
    j.Put(LINQ_UNSUPPORTED_MEMBER_TYPE,
    "メンバー型がサポートされていません。");
    j.Put(LINQ_UNSUPPORTED_CONSTANT,
    "'{0}'用の定数がサポートされていません。");
    j.Put(LINQ_NO_MAPPING,
    "{0}用のマップ列を検索できませんでした。");
    j.Put(LINQ_BAD_STORAGE_PROPERTY,
    "BadStorageProperty");
    j.Put(LINQ_UNHANDLED_EXPRESSION_TYPE,
    "ハンドルされていない式の型：'{0}'");
    j.Put(LINQ_UNHANDLED_BINDING_TYPE,
    "ハンドルされていない連結の型：'{0}'");
    j.Put(EF_NOT_EXPRESSIONS_UNSUPPORTED,
    "複合型式上のNot式がサポートされていません。");
    //ssr
    j.Put(SSRS_COMMAND_TYPE_ONLY,
    "この接続はCommandType.Textのみをサポートします。");
    j.Put(SSRS_TRANSACTIONS,
    "トランザクションがサポートされていません。");
    j.Put(SSRS_UNSUPPORTED_IMPERSONATION,
    "{0} SSRS Provider for {1}は、偽装をサポートしません。");
    j.Put(SSRS_INTEGRATED_SECURITY,
    "このプロバイダは、統合セキュリティをサポートしません。");
    j.Put(SSRS_USERPASS,
    "このプロバイダは、ユーザー名・パスワードを使用する認証処理をサポートしません。");
    j.Put(CANNOT_SET_PROVIDERNAME,
    "ProviderNameプロパティを設定できません。");
    j.Put(NULL_DATAREADER,
    "値 ");
    j.Put(NULL_DATAREADER_BUFFER,
    "バッファ");
    j.Put(HASROWS_NOT_SUPPORTED,
    "現時点では、HasRowsがサポートされていません。");
    j.Put(LINQ_NULL_QUERY,
    "クエリ");
    j.Put(KEYWORD_NOT_SUPPORTED,
    "キーワードがサポートされていません。'{0}'");
      
    //xls
    j.Put(XLS_APPLICATION_SETTINGS,
    "このプロバイダのためのアプリケーション設定はありません。");
    j.Put(XLS_TRIAL_INSERT,
    "評価版では、行の挿入機能が提供されません。挿入機能を有効にするには、製品ライセンスをご購入ください。");
    j.Put(XLS_TRIAL_DELETE,
    "評価版では、行の削除機能が提供されません。削除機能を有効にするには、製品ライセンスをご購入ください。");
    j.Put(XLS_TRIAL_UPDATE,
    "評価版では、行の更新機能が提供されません。更新機能を有効にするには、製品ライセンスをご購入ください。");
    j.Put(XLS_CONN_NOT_OPEN,
    "テストする前に接続を開いておく必要があります。");
    j.Put(XLS_INVALID_LICENSE,
    "本製品のライセンスは有効期限が切れたか、無効です。");
    j.Put("{0} Excel Add-In",
    "{0} Excel アドイン");
    j.Put(PROPERTY_MISSING,
    "{0} プロパティは初期化されていません。");
    
    //CacheProvider
    j.Put(CACHE_EMPTY_CONNECTION_STRING,
    "[{0}]：接続文字列を空にすることはできません。");
    j.Put(CACHE_CANNOT_LOAD_DRIVER,
    "適当なドライバをロードできません：[{0}]。詳細情報：{1}。");
    j.Put(CACHE_CANNOT_CONNECT,
    "プロバイダを使用して接続を確立できません：[{0}]。詳細情報：{1}。");
    j.Put(CACHE_DBPROVIDERFACTORIES_XAMARIN,
    "DbProviderFactoriesはXamarinに存在しません。");
    j.Put(CACHE_RANGE,
    "値[{0}]が予想範囲内に収まりません。");
    j.Put(NULL_CONNECTION_STRING,
    "接続文字列をnullにすることはできません。");
    j.Put(CACHE_RETRIEVE_METADATA,
    "データベースのメタデータを取得できません。");
    j.Put(NO_PREPARED_STATEMENT,
    "実行対象のプリペアドステートメントはありません。");
    j.Put(JAVADB_NULL,
    "JavaDbが開いていないため、コマンドを実行できません。");
    j.Put(JAVADB_CLOSED,
    "JavaDbが閉じているため、コマンドを実行できません。");
    j.Put(NO_RESULT_SET,
    "現時点では、利用可能な結果セットはありません。");
    j.Put(CACHEDRIVER,
    "CacheDriverが指定されていません。CacheDriverを構成してください。");
    j.Put(CACHECONNECTION,
    "CacheConnectionが指定されていません。CacheConnectionを構成してください。");
    j.Put(CACHING_IN_PROGRESS,
    "キャッシュ処理は実行中です。現在の処理が完了する前にキャッシュできません。");
    j.Put(CONNECTION_CLOSED,
    "最初に、接続する必要があります。");
    j.Put(SCHEMA_MODIFIED,
    "データをキャッシュできません。テーブルのスキーマが変更されています。");
    j.Put(CANNOT_ALTER_SCHEMA,
    "テーブルスキーマを変更できません。エラー：{0}");
    j.Put(CHANGESET_INACTIVE,
    "チェンジセットがアクティブである場合のみ、キャッシュが変更できます。");
    j.Put(CACHE_PRIMARY_KEYS_REQUIRED,
    "キャッシュされた行を更新するには、主キーが必要です。");
    j.Put(CACHE_PRIMARY_KEYS_NOT_DISCOVERED,
    "内部エラー：利用可能な主キーはありません。主キーをあらかじめ取得しておく必要があります。");
    j.Put(INVALID_DATA_TYPE,
    "無効なデータ型。");
    j.Put(INVALID_COL_DATA_TYPE,
    "無効な列データ型[{0}]。");
    j.Put(UNSUPPORTED_CULTURE_SETTING,
    "指定されたカルチャー設定[{0}]はサポートされていません。");
    j.Put(INVALID_CONNECTION_STRING,
    "入力された接続文字列が無効です。");
    j.Put(INVALID_CONNECTION_STRING_SYNTAX,
    "接続文字列の構文とインデックスが無効です：{0}。");
    j.Put(INVALID_CONNECTION_STRING_KEY,
    "接続文字列のキー名 [{0}]が無効です。");
    j.Put(NULL_POINTER_EXCEPTION,
    "Nullポインタの例外");
    j.Put(NOT_A_BOOLEAN,
    "{0}はブール値ではありません。");
    j.Put(RESULTSET_NOT_READY,
    "結果セットが準備できていません。");
    j.Put(INVALID_OUTPUT_PARAMETER,
    "インデックス[{0}]のパラメータは出力パラメータではありません。");
    j.Put(MAX_PARAMETERS_EXCEEDED,
    "プロシージャには、({0})以上のパラメータを設定することはできません。"); 
    j.Put(NO_PARAMETER_FOUND,
    "[{0}]というパラメータが見つかりません。");
    j.Put(UNSUPPORTED_FEATURE,
    "この機能はサポートされていません。");
    j.Put(BAD_NUMBER_FORMAT,
    "数値形式が正しくありません：[{0}]。");
    j.Put(METADATA_INDEX_OUT_OF_RANGE,
    "ParameterMetaData：インデックス[{0}]は範囲外です。");    
    j.Put(READONLY_DATASOURCE,
    "読み取り専用のデータソースはSELECTクエリのみをサポートします。");
    j.Put(CREATE_TABLE,
    "クエリが実行できません。[CREATE TABLE]がサポートされていません。");
    j.Put(TABLE_ALREADY_EXISTS,
    "テーブル[{0}]が既に存在しています。");
    j.Put(DROP_TABLE_UNSUPPORTED,
    "クエリが実行できません。[DROP TABLE]がサポートされていません。");
    j.Put(SQLPARSER_NULL,
    "このステートメントは指定されたコマンド型に対して無効です。");
    j.Put(CANNOT_BIND_PARAMETER,
    "パラメータが連結できません：{0}。");
    j.Put(UPDATE_READONLY_COLUMN,
    "[{0}]列は読み取り専用です。この列の値を更新できません。");
    j.Put(NUMERIC_COL_DATATYPE_INVALID,
    "小数点以下桁数と有効桁数の値はnumeric型であることが必要です。ご使用のスキーマを再確認してください。");
    j.Put(IGNORETYPES_INVALID,
    "IgnoreTypes接続プロパティの値が正しくありません。型のカンマ区切りの文字列が必須です。設定された値は{0}です。");
    j.Put(LOAD_RUNTIME_SETTINGS_FAILED,
    "ランタイム設定のロード中に失敗しました。指定されたファイルが存在しません。ファイル：{0}。");
    j.Put(LOCATION_DOES_NOT_EXIST,
    "接続文字列に指定されたファイルまたはフォルダが存在しません。場所 = {0}");
    j.Put(NO_TEST_CONNECTION,
    "このプロバイダは、テスト接続をサポートしません。");
    j.Put(INVALID_OBJECT_NAME,
            "オブジェクト名'{0}'が無効です。");
    j.Put(INVALID_COLUMN_NAME,
            "INVALID_COLUMN_NAME");
    j.Put(CANNOT_GET_INFO_ITEM,
    "情報アイテムを取得できません。");
    j.Put(INVALID_METHOD,
    "スクリプトに無効な{0}メソッドが見つかりました。");
    j.Put(CANNOT_RETRIEVE_COLUMNS,
    "[{0}]テーブルの列が取得できません。");
    j.Put(REFERENCE_MISSING_TABLE_AND_COLUMN,
    "参照({0})にはtablenameとcolnameが含まれていません。");
      j.Put(CLOSE_TEMPDB,
    "一時データベースを閉じる操作に失敗しました。エラー：{0}");
    j.Put(CREATE_TEMPDB,
    "一時データベースの作成に失敗しました。エラー：{0}");
    j.Put(QUERYCACHE_WITH_NONSQLITE_DRIVER,
    "接続を確立できません。QueryCacheは[{0}]のみで使用できます。");
    j.Put(NO_CACHE_CONNECTION,
    "キャッシュ接続を設定する必要があります。");
    j.Put(CONNECTION_QUERYCACHE_INT,
    "QueryCacheに数値を設定する必要があります。");
    j.Put(CONNECTION_SCHEMACACHEDURATION_INT,
    "SchemaCacheに数値を設定する必要があります。");
    j.Put(MAXCONNECTIONS_INT,
    "MaxConnectionsに整数値を設定する必要があります。");
    j.Put(CACHE_DB_TYPE_UNRECOGNIZED,
    "不明な'Cache DB Type'値が見つかりました：[{0}]。使用可能な値：'MySQL'、'SQLServer'、'Oracle'、'Generic'");
    j.Put(INDEX_OUT_OF_RANGE,
    "インデックスは範囲外です。インデックスの範囲は1から{0}までです。");
    j.Put(PARAM1_CANNOT_BE_FOUND_IN_METADATADB,
    "'{0}'テーブルは、メタデータのデータベースに見つかりません。");
    j.Put(ONE_READMODE,
    "指定できる値は１つのみです。利用可能な値はReadAsNullおよびReadAsEmptyです。");
    j.Put(INVALID_GETUPDATEBITS,
    "指定できる値は１つのみです。利用可能な値はUpdateToNull、UpdateToEmpty、およびIgnoreInUpdateです。");     
    j.Put(INVALID_GETINSERTBITS,
    "指定できる値は１つのみです。利用可能な値はInsertAsNull、InsertAsEmpty、およびIgnoreInInsertです。");
    j.Put(CONNECTION_MODE_INVALID,
    "不明なモードが検出されました：[{0}]");
    j.Put(CACHEPARTIAL_FALSE,
    "自動キャッシュは、すべての列（射影ではない）をキャッシュするクエリの場合のみ使用できます。部分的にキャッシュするには、CachePartial=Trueを設定する必要があります。");
    j.Put(UNABLE_TO_LOAD_PARAM1,
    "要求されたデータをロードできません。詳細情報：{0}");
    j.Put(EXEC_CACHE_NULL,
    "キャッシュが構成されていない場合、キャッシュからコマンドを実行できません。");
    j.Put(INVALID_QUERY,
    "無効なクエリ[{0}]。");
    j.Put(MULTIPLE_SYNC_BLOCKS,
    "複数の同期ブロックが検出されましたが、EnableMultipleSyncsはFalseに設定されています。");
    j.Put(TRANSACTION_NOT_SUPPORTED,
    "トランザクションがサポートされていません。");
    j.Put(BULKOPERATION_NOT_SUPPORTED,
    "バルク処理はサポートされていません。");
    j.Put(MIXBULKOPERATION_NOT_SUPPORTED ,
    "バルク処理タイプの混合はサポートされていません。");
    j.Put(UPDATEGRAMS_ERROR,
    "アップデートグラムエラー：{0}");
    j.Put(UPDATE_VIEW_FAILED,
    "ビューを更新できません。");
    j.Put(UNSUPPORTED_CACHE_QUERY,
    "サポートされていないキャッシュクエリ：{0}");
    j.Put(MISSING_TRANSACTION_ID,
    "トランザクションIDが不足しています。");
    j.Put(TRANSACTION_ALREADY_OPEN,
    "トランザクション[{0}]が既に開いています。");
    j.Put(TRANSACTION_DOES_NOT_EXIST,
    "トランザクション[{0}]が存在しません。");
    j.Put(TEMPDB_CREATE_TABLE,
            "一時データベースにテーブルを作成できません。詳細情報：{0}");
    j.Put(NO_SCHEMA,
            "スキーマを取得できません。");
    j.Put(NONTEXTCOMMAND_IN_CACHE,
    "キャッシュにテキスト以外のコマンドを実行しています。");  
    j.Put(UNKNOWN_FUNCTION,
    "不明な関数が検出されました。関数：'{0}'。");
    j.Put(UNKNOWN_DATATYPE,
    "不明なデータ型[{0}]。");
    j.Put(INVALID_COLUMN_INDEX,
    "列のインデックス[{0}]が無効です。");
    j.Put(TABLE_COLUMNS,
    "テーブルの列を取得できません。{0}");
    j.Put(TABLE_METADATA,
    "テーブルのメータデータを取得できません。エラー：{0}");
    j.Put(GETARRAY_INT,
    "getArray(int i)がサポートされていません。");
    j.Put(GETARRAY_STRING,
    "getArray(String colName)がサポートされていません。");
    j.Put(GETASCIISTREAM_INT,
    "getAsciiStream(int columnIndex)がサポートされていません。");
    j.Put(GETASCIISTREAM_STRING,
    "getAsciiStream(String columnName)がサポートされていません。");
    j.Put(RESULTSET_CURSOR,
    "結果セットを反復処理し、読み込む前に次の行を確認する必要があります。");
    j.Put(GETBINARYSTREAM_INT,
    "getBinaryStream(int columnIndex)がサポートされていません。");
    j.Put(GETBINARYSTREAM_STRING,
    "getBinaryStream(String columnName)がサポートされていません。");
    j.Put(GETBLOB_INT,
    "getBlob(int i)がサポートされていません。");
    j.Put(GETBLOB_STRING,
    "getBlob(String colName)がサポートされていません。");
    j.Put(GETCHARACTERSTREAM_INT,
    "getCharacterStream(int columnIndex)がサポートされていません。");
    j.Put(GETFETCHDIRECTION,
    "getFetchDirection()がサポートされていません。");
    j.Put(GETFETCHSIZE,
    "getFetchSize()がサポートされていません。");
    j.Put(GETOBJECT_INT,
    "getObject(int i, Map<String, Class<?> map)がサポートされていません。");
    j.Put(GETOBJECT_STRING,
    "getObject(String colName, Map<String, Class<?> map)がサポートされていません。");
    j.Put(GETREF_INT,
    "getRef(int i)がサポートされていません。");
    j.Put(GETREF_STRING,
    "getRef(String colName)がサポートされていません。");
    j.Put(GETUNICODESTREAM_STRING,
    "getUnicodeStream(String columnName)がサポートされていません。");
    j.Put(GETCLOB_INT,
    "getClob(int i)がサポートされていません。");
    j.Put(GETCLOB_STRING,
    "getClob(String colName)がサポートされていません。");
    j.Put(GETURL_INT,
    "getURL(int columnIndex)がサポートされていません。");
    j.Put(GETURL_STRING,
    "getURL(String columnName)がサポートされていません。");
    j.Put(GETUNICODESTREAM_INT,
    "getUnicodeStream(int columnIndex)がサポートされていません。");
    j.Put(METADATA_TABLES_READONLY,
    "メタデータテーブルではSELECTクエリのみ実行できます。");
    j.Put(PARAMETER_NOT_FOUND,
    "パラメータコレクションには、パラメータ{0}が見つかりませんでした。");
    j.Put(UNRECOGNIZED_COLUMN,
    "テーブル [{1}] のクエリで列 [{0}] が認識されません。");
    j.Put(OAUTHSETTINGS_READ,
    "OAuthSettingsの読み込み中にエラーが発生しました：[{0}]");
    j.Put(OAUTHSETTINGS_WRITE,
    "OAuthSettingsをディスクに書き込み中にエラーが発生しました：[{0}]");
    j.Put(INITIATEOAUTH,
    "OAuthを開始するには、'OAuthClientId'および'OAuthClientSecret'が必要です。");
    j.Put(INVALID_STATEMENT,
    "無効なステートメント。");
    j.Put(ARRAY_PARAM,
    "現時点では、配列型のパラメータがサポートされていません。");
    j.Put(ASCIISTREAM,
    "現時点では、ASCIIストリームのパラメータがサポートされていません。");
    j.Put(BIGDECIMAL,
    "現時点では、BigDecimal型のパラメータがサポートされていません。");
    j.Put(GETCHARACTERSTREAM_STRING,
    "getCharacterStream(String columnName)がサポートされていません。");
    j.Put(BINARYSTREAM,
    "現時点では、Binaryストリームのパラメータがサポートされていません。");
    j.Put(BLOB,
    "現時点では、Blob型のパラメータがサポートされていません。");
    j.Put(CHARACTERSTREAM,
    "現時点では、Characterストリームのパラメータがサポートされていません。");
    j.Put(CLOB,
    "現時点では、Clob型のパラメータがサポートされていません。");
    j.Put(REF_PARAM,
    "現時点では、Ref型のパラメータがサポートされていません。");
    j.Put(UNICODESTREAM,
    "現時点では、Unicodeストリームのパラメータがサポートされていません。");
    j.Put(CONNECTION_OPEN,
    "[{0}]接続を開くことができません。");
    j.Put(INVALID_STRING_OPERATOR,
    "'{0}'演算子を文字列型の列に使用できません。列：[{1}]。");
    j.Put(COLUMN_DOES_NOT_EXIST,
    "{0}列が存在しません。");
    j.Put(COLUMN_NOT_FOUND,
    "'{0}'列が見つかりません。");
    j.Put(SELECT_STAR,
    "'*'列を使用する場合、クエリ内の単一の列であることが必要です。");
    j.Put(USE_EXECUTEQUERY_WITH_SP,
    "ストアドプロシージャを実行する場合、executeQueryを使用してください。");
    j.Put(NO_SELECT_WITH_EXECUTEUPDATE,
    "executeUpdateがSELECTステートメントを受け取りません。代わりに、executeQueryを使用してください。");
    j.Put(CONNECTION_STRING_MODIFY_OPEN,
    "接続が開いている状態では接続文字列を変更できません。");
    j.Put(NEW_CHANGESET,
    "チェンジセットを完了する前に、新しいチェンジセットを開始できません。");
    j.Put(CREATESCHEMA,
    "スキーマ作成に関する手続きが見つかりません。");
    j.Put(REQUIRES_ONE_PARAM,
    "1つのパラメータを設定する必要があります。");
    j.Put(REQUIRES_INT_PARAM,
    "整数を設定する必要があります。[{0}]が提供されています。");
    j.Put(INVALID_DATETIME_WITH_PARAMS,
    "値は有効な日時ではありません。[{0}]が提供されています。");
    j.Put(INVALID_DATEPART_WITH_PARAMS,
    "datepartの文字列が無効です。[{0}]。");
    j.Put(EXPECTED_SELECT_OR_GETDELETED,
    "次のトークンはSELECTまたはGETDELETEDが必要ですが、[{0}]が見つかりました。");
    j.Put(ENDED_BEFORE_SP_NAME,
    "ストアドプロシージャの名前が必要ですが、ステートメントが途中で終了しました。");
    j.Put(ENDED_BEFORE_QUERY,
    "クエリが必要ですが、ステートメントが途中で終了しました。");
    j.Put(ENDED_BEFORE_CLOSING_PAREN,
    "')'の前でステートメントが途中で終了しました。");
    j.Put(EXPECTED_ANON_PARAM,
    "'?'が必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_COMMA,
    "','が必要ですが、{0}が見つかりました。");
    j.Put(SYNTAX,
    "'{0}'の近くで構文エラーが発生しました。");
    j.Put(INVALID_DATA_TYPE_WITH_PARAMS,
    "[{0}]は有効なデータ型ではありません。");
    j.Put(UNEXPECTED_TOKEN,
    "予想外のトークンが見つかりました：[{0}]。");
    j.Put(UNSUPPORTED_CONSTRAINT,
    "制約はまだサポートされていません。PRIMARY KEYのみが使用できます。");
    j.Put(SYNTAX_INT,
    "[{0}]の近くで構文エラーが発生しました。整数値が必要です。");
    j.Put(ENDED_BEFORE_COL_DEF,
    "列定義が必要ですが、ステートメントが途中で終了しました。");
    j.Put(NO_COL_DEF,
    "[{0}]列定義が見つかりません。");
    j.Put(EXPECTED_TOKEN,
    "{0}トークンが必要ですが、{2}値が設定されている{1}が見つかりました。");
    j.Put(UNKNOWN_TOKEN,
    "{1}位置上の解析処理中に不明な{0}トークンが見つかりました。");
    j.Put(EXPECTED_ENDQUOTE,
    "{1}位置にある引用符に対する終端の引用符{0}が見つかりませんでした。");
    j.Put(INPUT_READ_ERROR,
    "入力完了後、入力されたテキストを読み込みました。");
    j.Put(MALFORMED_SQL,
    "不正な形式のSQLステートメント：{0}\r\nステートメント：{1}");
    j.Put(SQL_COLCOUNT,
    "SQL列は一致していません。");
    j.Put(UPDATE_SELECT_STAR,
    "'*'列名はUPDATE-SELECTクエリに使用できません。");
    j.Put(EXPECTED_PRIMARY_KEY,
    "主キーが必要ですが、ステートメントが途中で終了しました。");
    j.Put(EXPECTED_IDENTIFIER,
    "識別子が必要ですが、{1}値が設定されている{0}が見つかりました。");
    j.Put(ENDED_BEFORE_PARAM1,
    "{0}が必要ですが、ステートメントが途中で終了しました。");
    j.Put(ENDED_BEFORE_TABLENAME,
    "テーブル名が必要ですが、ステートメントが途中で終了しました。");
    j.Put(EXECUTEQUERY_ACCEPTS_SELECT_ONLY,
    "executeQueryがSELECTステートメントのみを受け取ります。代わりに、executeUpdateを使用してください。");
    j.Put(ENDED_BEFORE_COLNAME_VALUE,
    "column=value式が必要ですが、ステートメントが途中で終了しました。");
    j.Put(USE_EXECUTEQUERY_WITH_SP,
    "ストアドプロシージャを実行する場合、executeQueryを使用してください。");
    j.Put(NO_SELECT_WITH_EXECUTEUPDATE,
    "executeUpdateがSELECTステートメントを受け取りません。代わりに、executeQueryを使用してください。");
    j.Put(ENDED_BEFORE_COLNAME,
    "列名が必要ですが、ステートメントが途中で終了しました。");
    j.Put(ENDED_BEFORE_DATATYPE,
    "データ型が必要ですが、ステートメントが途中で終了しました。");           
    j.Put(ENDED_BEFORE_COLSIZE,
    "列のサイズが必要ですが、ステートメントが途中で終了しました。");
    j.Put(ENDED_BEFORE_PARAMNAME_VALUE,
    "parameter=value式が必要ですが、ステートメントが途中で終了しました。");
    j.Put(EXPECTED_EQUALS,
    "'='が必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_TABLENAME_BEFORE_WHERE,
    "WHEREキーワードの前にテーブル名が必要です。");
    j.Put(INSERT_SELECT_STAR,
    "'*'列名はINSERT-SELECTクエリに使用できません。");
    j.Put(EXPECTED_START_PAREN,
    "'('が必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_VALUES,
    "'VALUES'が必要ですが、{0}が見つかりました。");
    j.Put(ENDED_BEFORE_EXPECTED_VALUE,
    "値が必要ですが、ステートメントが途中で終了しました。");
    j.Put(VALUE_CLAUSE_UNMATCHED,
    "VALUE句には、列一覧よりも多くの値が含まれています。");
    j.Put(EXPECTED_CLOSING_PAREN,
    "')'が必要ですが、ステートメントが途中で終了しました。");
    j.Put(EMPTY_STATEMENT,
    "空のステートメントです。");
    j.Put(INVALID_INT,
    "{0}は有効な整数値ではありません。");
    j.Put(PARSE_DOUBLE,
    "[{0}]ダブル値の解析中にエラーが発生しました。");
    j.Put(EXPECTED_FROM_FOUND_PARAM1,
    "FROMが必要ですが、{0}が見つかりました。");
    j.Put(MISSING_CLOSING_PAREN,
    "右かっこがありません。");
    j.Put(EXPECTED_ORDER,
    "ORDERが必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_FIRST_LAST_AFTER_NULLS,
    "NULLS の後にはFIRSTまたはLASTが必要です。");
    j.Put(EXPECTED_COLNAME_FOUND_PARAM1,
    "列名または','が必要ですが、'{0}'が見つかりました。");
    j.Put(EXPECTED_PARAM1_FOUND_PARAM2,
    "{0}が必要ですが、{1}が見つかりました。");
    j.Put(EXPECTED_NUM_FOUND_PARAM1,
    "','数値が必要ですが、'{0}'が見つかりました。");
    j.Put(EXPECTED_HAVING,
    "HAVINGが必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_GROUP,
    "GROUPが必要ですが、{0}が見つかりました。");
    j.Put(EXPECTED_COLNAME_OR_PARAM1_FOUND_PARAM2,
    "-列名、','または'{0}'が必要ですが、'{1}'が見つかりました。");
    j.Put(ENDED_BEFORE_COLNAME_PARSED,
    "列名を解析中にステートメントが予期せず終了しました。");
    j.Put(ENDED_BEFORE_COLNAME_FOUND,
    "テーブル名の後に列名が必要ですが、ステートメントが途中で終了しました。");
    j.Put(DELETE_ROW,
    "行を削除できませんでした。");
    j.Put(UPDATE_ROW,
    "行を更新できませんでした。");
    j.Put(INSERT_ROW,
    "行を挿入できませんでした。");    
    j.Put(EXPECTED_WHERE_FOUND_PARAM1,
    "WHEREの代わりに{0}が見つかりました。");
    j.Put(UNEXPECTED_END_AFTER_WHERE,
    "'WHERE'の後に基準が必要ですが、ステートメントが途中で終了しました。");
    j.Put(UNMATCHED_CLOSING_PAREN,
    "')'に一致する'('が見つかりません。");
    j.Put(UNMATCHED_OPENING_PAREN,
    "'('に一致する')'が見つかりません。");
    j.Put(OPERATOR_CANNOT_BE_NEGATED,
    "{0}演算子を否定できません。");
    j.Put(PAREN_LIST_FOLLOWS_PARAM1,
    "[{0}]の後は、かっこで囲まれる項目一覧が必要です。");
    j.Put(UNEXPECTED_END,
    "基準を解析中にステートメントが予期せず終了しました。");
    j.Put(UNEXPECTED_END_BEFORE_PARAM,
    "ステートメントが予期せず終了しました。パラメータが必要です。");
    j.Put(UNEXPECTED_END_BEFORE_END_PAREN,
    "ステートメントが予期せず終了しました。右かっこが必要です。");
    j.Put(EXPECTED_END_PAREN_FOUND_PARAM1,
    "右かっこが必要ですが、'{0}'が見つかりました。");
    j.Put(EXPECTED_DATATYPE,
    "[{0}]の近くで構文エラーが発生しました。データ型が必要です。");
    j.Put(EXPECTED_LEN,
    "[{0}]の近くで構文エラーが発生しました。長さが必要です。");
    j.Put(EXPECTED_SCALE,
    "[{0}]の近くで構文エラーが発生しました。小数点以下桁数が必要です。");
    j.Put(UNEXPECTED_DELIMITER,
    "予想外の区切り文字'{0}'が見つかりました。");
    j.Put(PARSE_INT,
    "[{0}]整数値の解析中にエラーが発生しました。");
    j.Put(NULL_ANALYZER,
    "アナライザー名はNULLです。");
    j.Put(UNSUPPORTED_ANALYZER,
    "'{0}'アナライザーはサポートされていません。");  
    j.Put(AT_LEAST_ONE_PARAM,
    "少なくとも1つのパラメータを設定する必要があります。");      
    j.Put(TOO_MANY_OPEN_CONNECTIONS,
    "開いている接続が多すぎます。プロバイダは、同時接続数が{0}を超えないように構成されています。ご使用後は接続を閉じることにご注意ください。");
    j.Put(EF_DESIGN_TIME_METADATA,
    "デザイン時の操作性を向上させるメタデータが作成できません。詳細情報：{0}");
    j.Put(REQUIRES_THREE_PARAMS,
    "3つのパラメータを設定する必要があります。");
    j.Put(INVALID_DATETIME_WITH_PARAMS,
    "値は有効な日時ではありません。[{0}]が提供されています。");
    j.Put(YEAR_UNSUPPORTED,
    "年はまだサポートされていません。");
    j.Put(QUARTER_UNSUPPORTED,
    "四半期はまだサポートされていません。");
    j.Put(MONTH_UNSUPPORTED,
    "月はまだサポートされていません。");
    j.Put(DAYOFYEAR_UNSUPPORTED,
    "年間積算日はまだサポートされていません。");
    j.Put(WEEK_UNSUPPORTED,
    "週はまだサポートされていません。");
    j.Put(WEEKDAY_UNSUPPORTED,
    "曜日はまだサポートされていません。");
    j.Put(MICROSECOND_UNSUPPORTED,
    "マイクロ秒はまだサポートされていません。");
    j.Put(NANOSECOND_UNSUPPORTED,
    "ナノ秒はまだサポートされていません。");
    j.Put(INVALID_YEAR,
    "年の値が無効です。");
    j.Put(INVALID_MONTH,
    "月の値が無効です。");
    j.Put(INVALID_DAY,
    "日付の値が無効です。");
    j.Put(TZOFFSET_UNSUPPORTED,
    "TZoffsetはまだサポートされていません。");
    j.Put(NULL_DATEPART,
    "datepartの値をnullにすることはできません。");
    j.Put(REQUIRES_TWO_PARAMS,
    "2つのパラメータを設定する必要があります。");
    j.Put(INVALID_DATETIME,
    "datetimeの文字列が無効です！");
    j.Put(INVALID_DATEPART,
    "datepartの文字列が無効です！");
    j.Put(AT_LEAST_TWO_PARAMS,
    "少なくとも2つのパラメータを設定する必要があります。");
    j.Put(AT_MOST_THREE_PARAMS,
    "最大で3つのパラメータを設定することができます。");
    j.Put(DATEFIRST_INT_RANGE,
    "datefirstパラメータの値は、1～7間の整数値である必要があります。");
    j.Put(INVALID_PRECISION,
    "有効桁数の値が無効です。");
    j.Put(INVALID_FRACTION,
    "小数部分の値が無効です。");
    j.Put(INVALID_SECONDS,
    "秒の値が無効です。");
    j.Put(INVALID_MINUTE,
    "分の値が無効です。");
    j.Put(INVALID_HOUR,
    "時間の値が無効です。");
    j.Put(REQUIRES_EIGHT_PARAMETERS,
    "8つのパラメータを設定する必要があります。");
    j.Put(INVALID_MILLISECONDS,
    "ミリ秒の値が無効です！");
    j.Put(REQUIRES_SEVEN_PARAMETERS,
    "7つのパラメータを設定する必要があります。");
    j.Put(MONTH_TO_ADD_INT,
    "month_to_addは整数値である必要があります。");
    j.Put("tart_date is not a valid date.",
    "tart_dateは有効な日付ではありません。");
    j.Put(INVALID_START_DATE,
    "start_dateは有効な日付ではありません。");
    j.Put(AT_MOST_TWO_PARAMS,
    "最大で2つのパラメータを設定することができます。");
    j.Put(POSITIVE_COUNT,
    "集計する値に正数を設定する必要があります。");
    j.Put(CAST_TO_FLOAT,
    "[{0}]を浮動小数点数にキャストできません。");
    j.Put(CAST_TO_INT,
    "[{0}]を整数にキャストできません。");
    j.Put(CAST_TO_NUM,
    "[{0}]を数値にキャストできません。");
    j.Put(REQUIRES_POSITIVE,
    "この引数に正数を設定する必要があります。[{0}]が提供されています。");
    j.Put(POSITIVE_REPEAT,
    "繰り返して集計するために正数を設定する必要があります。");
    j.Put(POSITIVE_CHAR_COUNT,
    "集計する文字の値に正数を設定する必要があります。");
    j.Put(REQUIRES_FIVE_PARAMS,
    "5つのパラメータを設定する必要があります。");
    j.Put(POSITIVE_DECIMAL,
    "小数のパラメータの値に正数を設定する必要があります。");
    j.Put(POSITIVE_LEN,
    "長さの値に正数を設定する必要があります。");
    j.Put(CONVERT,
    "変換する数値を設定する必要があります。");
    j.Put(START_INT,
    "開始位置の値に整数を指定する必要があります。");
    j.Put(LENGTH_INT,
    "長さの値に整数を指定する必要があります。");
    j.Put(POSITIVE_LEN,
    "長さの値に正数を指定する必要があります。");
    j.Put(NOARGS,
    "引数はありません。");
    j.Put(AT_LEAST_TWO_PARAMS,
    "2つのパラメータを設定することができます。");
    j.Put(REQUIRES_TWO_PARAMS,
    "2つのパラメータを設定する必要があります。");
    j.Put(NO_SUCH_TABLE,
    "[{0}]テーブルが存在しません。");
    j.Put(UNRECOGNIZED_KEYWORD,
    "不明なキーワード：{0}");
    j.Put(JAVA_PARAMETER_EXCEPTION,
    "パラメータインデックスが無効です： {0}。インデックスは0より大きく、クエリで指定されたパラメータ数を超えないようにする必要があります。");
    j.Put(INVALID_CACHE_OPTION,
    "キャッシュオプションが無効です {0}。これはREPLICATEでのみ利用可能です。");
    //SqlParser
    j.Put(NOT_FOUND_DIALECT_SQL_BUILDER,
            "'{0}' dialect SqlBuilder が見つかりません。");
    j.Put(NO_SUPPORT_SQL_NORMALIZATION,
            "SQL '{0}'の正規化はサポートされていません。");
    j.Put(NO_TABLE_REFERENCE_INCLUDE_IN_FROM,
            "FROM に含める'{0}'のテーブル参照はありません。");
    j.Put(NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR,
            "SqlConditionの左式または右式の削除はサポートされていません。");
    j.Put(UNKNOWN_COLUMN_IN_FIELDS_LIST,
	        "不明なカラム'{0}'。");
    j.Put(AMBIGUOUS_COLUMN_IN_FIELDS_LIST,
			"カラム'{0}'は不明です。");

    //ODBC
    j.Put(ODBC_CANNOT_CONNECT,
    "サーバーに接続できません。構成を確認してサーバーへの接続をもう一度実行してください。");
    j.Put(CONNECTION_SUCCESSFUL,
    "接続テストに成功しました。");
    j.Put(CANNOT_OPEN_HELP,
    "ヘルプを開くことができません。");
    j.Put(CANNOT_OPEN_HELP_WITH_PARAMS,
    "ヘルプ'{0}'を開くことができません。");
    j.Put(INVALID_DSN,
    "データソース名'{0}'が無効です。ODBC Setup");
    j.Put(NATIVE_CODE,
    "すべての関数は、ネイティブコードで返される必要があります。");
    //XLS
    j.Put(NO_PROVIDER,
    "'{0}'というプロバイダはありません。");
    j.Put(NO_UPDATE,
    "Updateステートメントを入力してください。");
    j.Put(NO_INSERT,
    "Insertステートメントを入力してください。");
    j.Put(NO_DELETE,
    "Deleteステートメントを入力してください。");
    j.Put(NO_SELECT,
    "Selectステートメントを入力してください。");
    j.Put(ARRAY_ARG,
    "引数は配列である必要があります。");
    j.Put("Transaction has already been committed or is not pending",
    "トランザクションは既にコミットされているか、保留中ではありません。");
    j.Put("Invalid parameter index: {0}. Index must be greater than 0 and no more than the number of parameters specified in the query.",
    "パラメータインデックスが無効です: {0}。インデックスは0より大きく、クエリで指定されたパラメータ数を超えないようにする必要があります。");
    j.Put(NO_SUPPORT_OAUTH_NOPROMPT,
    "OAuth認証はサポートされていません。");
    j.Put(CURSOR_MOVE_PRIVIOUS_UNSUPPORTED,
    "現在のカーソルでは前の移動はサポートされていません。");
    j.Put(NO_SUPPORT_EQUAL_IN_DATELITERALS,
    "日付リテラルではイコールはサポートされていません。");
    j.Put(INVALID_PARAMETER_IN_LITERAL,
            "リテラル '{0}'のパラメータが無効です。");
    j.Put(INVALID_TYPE_FORMATTER,
            "データ型フォーマッタが無効です。");
    j.Put(WARN_STRING,
            "Warning: {0}");
 }

 public string GetLocalizedString(string language, string message) {
    string retVal = message;
    if( language.Equals(RSBLocalizedException.JAPANESE) ) {
      string local = j.Get(message);
      if(local != null ) retVal = local; 
    }
    return retVal;
  }
  
  public static string LocalizedMessage(string msg,
               
               params Object[] args
                ) {
     return RSBLocalizedException.LocalizedMessage(SqlExceptions.INSTANCE, msg, args);
  }
  
  public static RSBException Exception(string code, string msg,
               
               params Object[] args
                ) {
     return RSBLocalizedException.GetException(SqlExceptions.INSTANCE, code, msg, args);
  }
}
}

