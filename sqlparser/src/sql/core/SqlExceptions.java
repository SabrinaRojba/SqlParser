package core;
/*#
using RSSBus.core;
using RSSBus;
#*/

import rssbus.RSBException;
import rssbus.oputils.common.LangDictionary;
import rssbus.oputils.common.RSBLocalizedException;
import java.util.Hashtable;

public class SqlExceptions implements LangDictionary {
//cnx
  public static final String EF_SQLGEN_STATEMENT = "SQL generation for {0} statements is not supported.";
  public static final String EF_COMMANDTREE_WITH_PARAMS = "Error generating SQL for command tree of type {0}\r\n. Exception was: {1}";
  public static final String EF_NONPRIMITIVE_TYPE =  "Cannot create parameter of nonprimitive type";
  public static final String SCAN_DEFININGQUERY = "Scan Expressions backed by DefiningQuery are not supported.";
  public static final String EF_SQLGEN_EXPRESSION = "SQL generation for {0} expressions is not supported.";
  public static final String EF_SQLGEN_NEWINSTANCE = "NewInstance expressions without a row type are not supported.";
  public static final String LINQ_NO_TABLE_ATTRIBUTE = "{0} is not decorated with the TableAttribute.";
  public static final String LINQ_NO_TABLE_FOR_TYPE = "It was not possible to get a table for type {0}";
  public static final String LINQ_NULL_QUERY = "Null value for query.";
  public static final String EF_UNKNOWN_PRIMITIVE_TYPE = "Unknown primitive type {0}";
  public static final String EF_UNSUPPORTED_INFORMATION_TYPE = "The provider does not support the information type {0}.";
  public static final String EF_UNSUPPORTED_TYPE = "The underlying provider does not support the type {0}.";
  public static final String EF_NO_STORE_TYPE_FOR_EDM_TYPE = "There is no store type corresponding to the EDM type '{0}' of primitive type '{1}'.";
  public static final String EF_MANIFEST = "Could not create command: The DbProviderManifest input was null.";
  public static final String EF_COMMANDTREE = "Could not create command: The DbCommandTree input was null.";
  public static final String EF_SQLGEN_SELECT = "SQL generation is not available for SELECT statements.";
  public static final String LINQ_NOT_MARKED_AS_TABLE = "{0} is not marked as a table.";
  public static final String LINQ_NULL_EXPRESSION_TREE = "Could not create query: The expression tree was null.";
  public static final String LINQ_NO_QUERYABLE_ARGUMENT = "Could not create query: A queryable argument is required.";
  public static final String LINQ_NO_MATCHING_ENTITY = "No entity was found meeting the specified criteria.";
  public static final String LINQ_UNSUPPORTED_EXPRESSION = "This query expression is not supported.";
  public static final String EF_UNSUPPORTED_OPERATOR = "Operator {0} is not supported.";
  public static final String LINQ_NO_ITEMS_FOR_PROJECTION = "There are no items for projection in this query.";
  public static final String LINQ_NO_MULTIPLE_RESULT_SHAPES = "Multiple result shapes are not supported.";
  public static final String LINQ_NOT_QUERYABLE = "Invalid sequence operator call. The type for the operator is not queryable.";
  public static final String LINQ_MULTIPLE_WHERE_CLAUSES = "You cannot have more than one where clause in the expression.";
  public static final String LINQ_MULTIPLE_SELECT = "You cannot have more than one select statement in the expression.";
  public static final String LINQ_MULTITAKE_VALUE = "You cannot have more than 1 Take/First/FirstOrDefault in the expression";
  public static final String LINQ_UNARY = "The unary operator {0} is not supported";
  public static final String LINQ_BINARY_WHERE = "The where predicate does not support {0} as a binary operator";
  public static final String LINQ_WHERE_CLASS = "The member {0} in the where expression cannot be found on the {1} class";
  public static final String LINQ_SUBQUERIES = "Subqueries are not supported.";
  public static final String LINQ_NONMEMBER = "The member '{0}' is not supported";
  public static final String LINQ_NULL_METATABLE = "It was not possible to get metadata for {0}.";
  public static final String LINQ_NULL_METAMEMBER = "The member {0} in the select expression cannot be found on the {1} class or is not selectable.";
  public static final String LINQ_UNSUPPORTED_MEMBER_TYPE = "The member type is not supported.";
  public static final String LINQ_UNSUPPORTED_CONSTANT = "The constant for '{0}' is not supported";
  public static final String LINQ_NO_MAPPING = "It was not possible to find a mapping column for {0}";
  public static final String LINQ_BAD_STORAGE_PROPERTY = "Bad storage property";
  public static final String LINQ_UNHANDLED_BINDING_TYPE = "Unhandled binding type '{0}'";
  public static final String EF_SQL_GEN_EXPRESSION = "SQL generation for {0} expressions is not supported.";
  public static final String EF_SQLGEN_NEW_INSTANCE = "NewInstance expressions without a row type are not supported.";
  public static final String EF_NOT_EXPRESSIONS_UNSUPPORTED = "Not Expressions on complex expressions are not supported.";
  public static final String EF_SQLGEN_UNSUPPORTED_RESULT_TYPE = "Nested record types or complex objects are not supported.";
  public static final String EF_SQLGEN_NONCONSTANT_VALUE_IN_LIMIT = "Limit expressions are only supported with constant values.";
  public static final String LINQ_UNHANDLED_EXPRESSION_TYPE = "Unhandled expression type '{0}'";
  public static final String SSRS_COMMAND_TYPE_ONLY = "This connection supports CommandType.Text only.";
  public static final String SSRS_TRANSACTIONS = "Transactions not supported";
  public static final String SSRS_UNSUPPORTED_IMPERSONATION = "Impersonation is not supported in {0} SSRS Provider for {1}";
  public static final String SSRS_INTEGRATED_SECURITY = "Integrated Security is not supported for this provider.";
  public static final String SSRS_USERPASS = "Username/Password based authentication is not supported.";
  public static final String CANNOT_SET_PROVIDERNAME = "Cannot set ProviderName property";
  public static final String NULL_DATAREADER = "DataReader values are null.";
  public static final String NULL_DATAREADER_BUFFER = "DataReader buffer is null.";
  public static final String HASROWS_NOT_SUPPORTED = "HasRows is not supported at this time.";
  public static final String KEYWORD_NOT_SUPPORTED = "Keyword not supported: '{0}'";
  public static final String XLS_APPLICATION_SETTINGS = "No application settings for this provider.";
  public static final String XLS_TRIAL_INSERT = "Ability to insert rows is not available in the trial version. A full license is required to enable inserts.";
  public static final String XLS_TRIAL_DELETE = "Ability to delete rows is not available in the trial version. A full license is required to enable deletes.";
  public static final String XLS_TRIAL_UPDATE =  "Ability to update rows is not available in the trial version. A full license is required to enable updates.";
  public static final String XLS_CONN_NOT_OPEN = "Connection could not be tested: Connection not open.";
  public static final String XLS_INVALID_LICENSE = "The license for this product has already expired or is not valid";
  public static final String XLS_REGKEY = "{0} Excel Add-In";
  public static final String PROPERTY_MISSING = "{0} property has not been initialized";
  //code
  //XLS
  public static final String CACHE_EMPTY_CONNECTION_STRING = "[{0}]: The connection string cannot be empty";
  public static final String CACHE_CANNOT_LOAD_DRIVER = "Unable to load the appropriate driver [{0}]. Details: {1}";
  public static final String CACHE_CANNOT_CONNECT = "Unable to establish a connection using provider [{0}]. Details: {1}";
  public static final String CACHE_DBPROVIDERFACTORIES_XAMARIN = "DbProviderFactories does not exist in Xamarin.";
  public static final String CACHE_RANGE = "Value [{0}] does not fall within the expected range,";
  public static final String NULL_CONNECTION_STRING = "Connection string cannot be null.";
  public static final String CACHE_RETRIEVE_METADATA = "Unable to retrieve the database metadata.";
  public static final String NO_PREPARED_STATEMENT = "No prepared statement to execute.";
  public static final String JAVADB_NULL = "Commands could not be executed because the Java DB is not open.";
  public static final String JAVADB_CLOSED = "Commands could not be executed becauase the Java DB is closed.";
  public static final String NO_RESULT_SET = "No result set is currently available.";
  public static final String CACHEDRIVER = "No driver for the cache database was configured. Set this with the CacheDriver property.";
  public static final String CACHECONNECTION = "The connection string to the cache was not configured. Set this with the CacheConnection property.";
  public static final String CACHING_IN_PROGRESS = "Cannot cache until the previous caching operation has completed.";
  public static final String CONNECTION_CLOSED = "You must first connect.";
  public static final String SCHEMA_MODIFIED = "Unable to cache data. The table schema has been modified.";
  public static final String CANNOT_ALTER_SCHEMA = "Unable to alter the table schema. Error: {0}";
  public static final String CHANGESET_INACTIVE = "Can only modify cache while in a change set that is active";
  public static final String CACHE_PRIMARY_KEYS_REQUIRED = "Primary keys are required to update cached rows.";
  public static final String CACHE_PRIMARY_KEYS_NOT_DISCOVERED = "Internal Error: Primary keys were not available.";
  public static final String INVALID_DATA_TYPE = "Invalid data type";
  public static final String INVALID_COL_DATA_TYPE = "Invalid column data type [{0}].";
  public static final String UNSUPPORTED_CULTURE_SETTING = "The culture setting [{0}] is not supported.";
  public static final String INVALID_CONNECTION_STRING = "The connection string is invalid.";
  public static final String INVALID_CONNECTION_STRING_SYNTAX = "Invalid connection string syntax at index: {0}";
  public static final String INVALID_CONNECTION_STRING_KEY = "Invalid connection string key name [{0}]";
  public static final String NULL_POINTER_EXCEPTION = "Null pointer exception";
  public static final String NOT_A_BOOLEAN = "{0} is not a boolean value";
  public static final String RESULTSET_NOT_READY = "Result set is not ready.";
  public static final String INVALID_OUTPUT_PARAMETER = "Parameter at index [{0}] is not an output parameter.";
  public static final String NO_PARAMETER_FOUND = "No parameter named [{0}] was found.";
  public static final String UNSUPPORTED_FEATURE = "Feature is not supported.";
  public static final String BAD_NUMBER_FORMAT = "Bad number format: [{0}].";
  public static final String MAX_PARAMETERS_EXCEEDED = "Procedure may have at most ({0}) parameters.";
  public static final String METADATA_INDEX_OUT_OF_RANGE = "Index [{0}] in the parameter metadata is out of range.";
  public static final String READONLY_DATASOURCE = "The read-only datasource supports only SELECT queries.";
  public static final String CREATE_TABLE = "The query cannot be executed: [CREATE TABLE] is not supported.";
  public static final String TABLE_ALREADY_EXISTS = "The table [{0}] already exists.";
  public static final String DROP_TABLE_UNSUPPORTED = "The query cannot be executed: [DROP TABLE] is not supported.";
  public static final String SQLPARSER_NULL = "Invalid statement for the command type specified.";
  public static final String CANNOT_BIND_PARAMETER = "Unable to bind the parameter: {0}.";
  public static final String UPDATE_READONLY_COLUMN = "Column [{0}] could not be updated. This column is read-only.";
  public static final String NUMERIC_COL_DATATYPE_INVALID = "Invalid schema configuration: The scale and size (precision) must be numeric.";
  public static final String MAX_PRECISION_REQUIRES_INT = "Incorrect value for the connection property Max Precision. It requires an integer. The following value was provided: {0}.";
  public static final String EXECUTE_FAILED = "Could not execute the specified command: {0}";
  public static final String IGNORETYPES_INVALID = "Incorrect value for the connection property Ignore Types. It must be a comma-separated string of types. The following value was provided: [{0}].";
  public static final String LOAD_RUNTIME_SETTINGS_FAILED = "Loading runtime settings failed. The file specified does not exist. File: {0}.";
  public static final String LOCATION_DOES_NOT_EXIST = "The file or the folder provided in the connection string does not exist. Location: {0}";
  public static final String NO_TEST_CONNECTION = "This provider does not support testing connections.";
  public static final String INVALID_OBJECT_NAME = "Invalid object name '{0}'.";
  public static final String INVALID_COLUMN_NAME = "Invalid column name '{0}'.";
  public static final String CANNOT_GET_INFO_ITEM = "Unable to get the info item.";
  public static final String INVALID_METHOD = "Invalid method {0} found in the script.";
  public static final String CANNOT_RETRIEVE_COLUMNS = "Unable to retrieve columns for table [{0}].";
  public static final String REFERENCE_MISSING_TABLE_AND_COLUMN = "Reference [{0}] does not contain table name and column name.";
  public static final String NO_CACHE_CONNECTION = "The Cache Connection property was not set.";
  public static final String CONNECTION_QUERYCACHE_INT = "The QueryCache property requires a numeric value.";
  public static final String CONNECTION_SCHEMACACHEDURATION_INT = "The SchemaCacheDuration property requires an integer value.";
  public static final String MAXCONNECTIONS_INT = "The MaxConnections property requires an integer value.";
  public static final String CACHE_DB_TYPE_UNRECOGNIZED = "The value for Cache DB Type was not recognized: [{0}]. Possible values are 'MySQL', 'SQLServer', 'Oracle', and 'Generic'.";
  public static final String CREATE_TEMPDB = "Temp database creation failed. Error: {0}";
  public static final String CLOSE_TEMPDB = "Temp DB failed to close. Error: {0}";
  public static final String EF_DESIGN_TIME_METADATA = "The metadata used for additional design-time integration could not be loaded.";
  public static final String INDEX_OUT_OF_RANGE = "Index out of range: The index must be between 1 and {0}.";
  public static final String QUERYCACHE_WITH_NONSQLITE_DRIVER = "Unable to establish a connection. The following drivers are supported with QueryCache: [{0}].";
  public static final String PARAM1_CANNOT_BE_FOUND_IN_METADATADB = "The table '{0}' was not found in the metadata database.";
  public static final String ONE_READMODE = "Only one value can be specified. Possible values are ReadAsNull and ReadAsEmpty.";
  public static final String INVALID_GETUPDATEBITS = "Only one value can be specified. Possible values are UpdateToNull, UpdateToEmpty, and IgnoreInUpdate.";
  public static final String INVALID_GETINSERTBITS = "Only one value can be specified. Possible values are InsertAsNull, InsertAsEmpty, and IgnoreInInsert.";
  public static final String CONNECTION_MODE_INVALID = "Unknown mode encountered: [{0}]";
  public static final String CACHEPARTIAL_FALSE = "The AutoCache property can only be used for queries that cache all columns (not a projection). To cache partially, set CachePartial=True in addition to AutoCache.";
  public static final String UNABLE_TO_LOAD_PARAM1 = "Unable to load the requested data. Detail: {0}";
  public static final String EXEC_CACHE_NULL = "Cannot execute command: Cache has not been configured.";
  public static final String INVALID_QUERY = "Invalid query [{0}].";
  public static final String UNSUPPORTED_CACHE_QUERY = "Unsupported cache query: {0}";
  public static final String MISSING_TRANSACTION_ID = "Transaction Id was missing.";
  public static final String TRANSACTION_ALREADY_OPEN = "Transaction [{0}] is already open.";
  public static final String TRANSACTION_DOES_NOT_EXIST = "The transaction [{0}] does not exist.";
  public static final String MULTIPLE_SYNC_BLOCKS = "Multiple sync blocks were detected but EnableMultipleSyncs was set to false.";
  public static final String TRANSACTION_NOT_SUPPORTED = "Transaction is not supported.";
  public static final String BULKOPERATION_NOT_SUPPORTED = "Bulk operations are not supported.";
  public static final String MIXBULKOPERATION_NOT_SUPPORTED = "Mixing bulk operation types is not supported.";
  public static final String UPDATEGRAMS_ERROR = "Updategrams error: {0}";
  public static final String UPDATE_VIEW_FAILED = "Views cannot be updated.";
  public static final String TEMPDB_CREATE_TABLE = "Unable to create the table in the temp database. Detail: {0}";
  public static final String NO_SCHEMA = "Unable to get schema.";
  public static final String NONTEXTCOMMAND_IN_CACHE = "Nontext commands cannot be executed in the cache.";
  public static final String UNKNOWN_FUNCTION = "Unknown function: {0}";
  public static final String UNKNOWN_DATATYPE = "Unknown datatype: {0}.";
  public static final String INVALID_COLUMN_INDEX = "Invalid column index [{0}]";
  public static final String TABLE_COLUMNS = "Unable to get table columns. {0}";
  public static final String TABLE_METADATA = "Unable to get table metadata. Error: {0}";
  public static final String GETARRAY_INT = "getArray(int i) is not supported.";
  public static final String GETARRAY_STRING = "getArray(String colName) is not supported.";
  public static final String GETASCIISTREAM_INT = "getAsciiStream(int columnIndex) is not supported.";
  public static final String GETASCIISTREAM_STRING = "getAsciiStream(String columnName) is not supported.";
  public static final String RESULTSET_CURSOR = "You must iterate through the result set and check for the next row before reading from it.";
  public static final String GETBINARYSTREAM_INT = "getBinaryStream(int columnIndex) is not supported.";
  public static final String GETBINARYSTREAM_STRING = "getBinaryStream(String columnName) is not supported.";
  public static final String GETBLOB_INT = "getBlob(int i) is not supported.";
  public static final String GETBLOB_STRING = "getBlob(String colName) is not supported.";
  public static final String GETCHARACTERSTREAM_INT = "getCharacterStream(int columnIndex) is not supported.";
  public static final String GETCHARACTERSTREAM_STRING = "getCharacterStream(String columnName) is not supported.";
  public static final String GETCLOB_INT = "getClob(int i) is not supported.";
  public static final String GETCLOB_STRING = "getClob(String colName) is not supported.";
  public static final String GETFETCHDIRECTION = "getFetchDirection() is not supported.";
  public static final String GETFETCHSIZE = "getFetchSize() is not supported.";
  public static final String GETOBJECT_INT = "getObject(int i, Map<String, Class<?>> map) is not supported.";
  public static final String GETOBJECT_STRING = "getObject(String colName, Map<String, Class<?>> map) is not supported.";
  public static final String GETREF_INT = "getRef(int i) is not supported.";
  public static final String GETREF_STRING = "getRef(String colName) is not supported.";
  public static final String GETUNICODESTREAM_INT = "getUnicodeStream(int columnIndex) is not supported.";
  public static final String GETUNICODESTREAM_STRING = "getUnicodeStream(String columnName) is not supported."; 
  public static final String GETURL_INT = "getURL(int columnIndex) is not supported.";
  public static final String GETURL_STRING = "getURL(String columnName) is not supported.";
  public static final String METADATA_TABLES_READONLY = "Only SELECT queries can be executed on metadata tables.";
  public static final String PARAMETER_NOT_FOUND = "Parameter {0} was not found in the parameter collection.";
  public static final String UNRECOGNIZED_COLUMN = "Unrecognized column [{0}] in query for table [{1}].";
  public static final String OAUTHSETTINGS_READ = "An error was encountered when reading the OAuthSettings: [{0}]";
  public static final String OAUTHSETTINGS_WRITE = "An error was encountered when writing the OAuthSettings to disk: [{0}]";  
  public static final String INITIATEOAUTH = "'OAuthClientId' and 'OAuthClientSecret' are needed to initiate OAuth.";
  public static final String INVALID_STATEMENT = "Invalid statement.";
  public static final String ARRAY_PARAM = "Array type parameters are not currently supported";
  public static final String ASCIISTREAM = "ASCII stream parameters are not currently supported";
  public static final String BIGDECIMAL = "Big Decimal type parameters are not currently supported";
  public static final String BINARYSTREAM = "Binary stream parameters are not currently supported";
  public static final String BLOB = "Blob type parameters are not currently supported";
  public static final String CHARACTERSTREAM = "Character stream parameters are not currently supported";
  public static final String CLOB = "Clob type parameters are not currently supported";
  public static final String REF_PARAM = "Ref type parameters are not currently supported.";
  public static final String UNICODESTREAM = "Unicode stream parameters are not currently supported";
  public static final String CONNECTION_OPEN =  "Unable to open the connection [{0}].";
  public static final String PARSE_INT = "Error parsing int value [{0}].";
  public static final String NULL_ANALYZER = "Analyzer name is NULL.";
  public static final String UNSUPPORTED_ANALYZER = "Analyzer '{0}' is not supported";
  public static final String AT_LEAST_ONE_PARAM = "At least one parameter is required.";
  public static final String TOO_MANY_OPEN_CONNECTIONS = "Too many open connections. You have configured the provider to have no more than {0} connections open at a time. Ensure that connections are closed after use.";
  public static final String REQUIRES_THREE_PARAMS = "You must set three parameters.";
  public static final String INVALID_DATETIME_WITH_PARAMS = "The value is not a valid datetime. Provided [{0}].";
  public static final String YEAR_UNSUPPORTED = "Year is not yet supported.";
  public static final String QUARTER_UNSUPPORTED = "Quarter is not yet supported.";
  public static final String MONTH_UNSUPPORTED = "Month is not yet supported.";
  public static final String DAYOFYEAR_UNSUPPORTED = "Dayofyear is not yet supported.";
  public static final String WEEK_UNSUPPORTED = "Week is not yet supported.";
  public static final String WEEKDAY_UNSUPPORTED = "Weekday is not yet supported.";
  public static final String MICROSECOND_UNSUPPORTED = "Microsecond is not yet supported.";
  public static final String NANOSECOND_UNSUPPORTED =  "Nanosecond is not yet supported.";
  public static final String INVALID_DATEPART_WITH_PARAMS = "Invalid date part string [{0}].";
  public static final String INVALID_YEAR = "Year is not a valid value.";
  public static final String INVALID_MONTH = "Month is not a valid value.";
  public static final String INVALID_DAY = "Day is not a valid value.";
  public static final String TZOFFSET_UNSUPPORTED = "TZoffset is not yet supported.";
  public static final String REQUIRES_TWO_PARAMS = "Two parameters are required.";
  public static final String INVALID_DATETIME = "Invalid date-time string.";
  public static final String INVALID_DATEPART = "Invalid date-part string.";
  public static final String NULL_DATEPART = "The date part cannot be null.";
  public static final String AT_LEAST_TWO_PARAMS = "You must set at least two parameters.";
  public static final String AT_MOST_THREE_PARAMS = "You can set at most three parameters.";
  public static final String DATEFIRST_INT_RANGE = "The datefirst parameter must be an integer from 1 to 7.";
  public static final String INVALID_PRECISION = "Precision parameter is not valid.";
  public static final String INVALID_FRACTION = "Fraction is not valid.";
  public static final String INVALID_SECONDS = "Seconds is not a valid value.";
  public static final String INVALID_MINUTE = "Minutes is not a valid value.";
  public static final String INVALID_HOUR = "Hour is not a valid value.";
  public static final String REQUIRES_EIGHT_PARAMETERS = "Eight parameters are required.";
  public static final String INVALID_MILLISECONDS = "Milliseconds are not a valid value.";
  public static final String REQUIRES_SEVEN_PARAMETERS = "You must set seven parameters.";
  public static final String MONTH_TO_ADD_INT = "The value for month_to_add must be an integer.";
  public static final String INVALID_START_DATE = "The value for start_date is not a valid date.";
  public static final String AT_MOST_TWO_PARAMS = "You must set two parameters.";
  public static final String POSITIVE_COUNT = "The count value requires a positive number.";
  public static final String CAST_TO_FLOAT = "Unable to cast [{0}] to a float.";
  public static final String CAST_TO_INT = "Unable to cast [{0}] to an integer.";
  public static final String CAST_TO_NUM = "Unable to cast [{0}] to number.";
  public static final String REQUIRES_POSITIVE = "This argument requires a positive number. Provided: [{0}].";
  public static final String NOARGS = "No parameters are accepted.";
  public static final String POSITIVE_REPEAT = "You must provide a positive number for the repeat count.";
  public static final String POSITIVE_CHAR_COUNT = "You must provide a positive number for the characters to count.";
  public static final String REQUIRES_FIVE_PARAMS = "You must set five parameters.";
  public static final String POSITIVE_DECIMAL = "You must provide a positive number for the decimal parameter.";
  public static final String POSITIVE_LEN = "You must specify a positive number for the length.";
  public static final String CONVERT = "You must provide a number to convert.";
  public static final String START_INT = "You must specify an integer for the start position.";
  public static final String LENGTH_INT = "You must specify an integer for the length.";
  public static final String NO_SUCH_TABLE = "No such table [{0}].";
  public static final String UNRECOGNIZED_KEYWORD = "Unrecognized keyword: {0}";
  public static final String ODBC_CANNOT_CONNECT = "Cannot connect to the server: invalid configuration.";
  public static final String CANNOT_OPEN_HELP = "The help file cannot be opened.";
  public static final String CANNOT_OPEN_HELP_WITH_PARAMS = "The help file '{0}' cannot be opened.";
  public static final String INVALID_DSN = "The Data Source Name '{0}' is invalid. ODBC Setup";
  public static final String NATIVE_CODE = "All functions should be returned in native code.";
  public static final String NO_PROVIDER = "There is not a provider named [{0}].";
  public static final String NO_UPDATE = "Input an update statement.";
  public static final String NO_INSERT = "Input an insert statement.";
  public static final String NO_DELETE = "Input a delete statement.";
  public static final String NO_SELECT = "Input a select statement.";
  public static final String ARRAY_ARG = "Argument must be an array.";
  public static final String EXPECTED_COMMA = "Expected ',', found '{0}'";
  public static final String UNEXPECTED_DELIMITER = "Unexpected delimiter '{0}'.";
  public static final String EXPECTED_SCALE = "Syntax error near [{0}]. Expected scale.";
  public static final String EXPECTED_LEN = "Syntax error near [{0}]. Expected length.";
  public static final String EXPECTED_DATATYPE = "Syntax error near [{0}]. Expected datatype.";
  public static final String EXPECTED_END_PAREN_FOUND_PARAM1 = "Expected end parenthesis. Found {0}.";
  public static final String UNEXPECTED_END_BEFORE_END_PAREN = "Unexpected end of statement. Expected end parenthesis.";
  public static final String UNEXPECTED_END_BEFORE_PARAM = "Unexpected end of statement. Expected a parameter.";
  public static final String UNEXPECTED_END = "Unexpected end of statement while parsing criteria.";
  public static final String EXPECTED_COLNAME_OR_PARAM1_FOUND_PARAM2 = "Expected column name, ',', or '{0}'. Found '{1}'.";
  public static final String PAREN_LIST_FOLLOWS_PARAM1 = "The [{0}] must be followed by a list of items enclosed in parentheses.";
  public static final String OPERATOR_CANNOT_BE_NEGATED = "Unable to negate the operator {0}";
  public static final String UNMATCHED_OPENING_PAREN = "Found '(' with no matching ')'.";
  public static final String UNMATCHED_CLOSING_PAREN = "Found ')' with no matching '('.";
  public static final String UNEXPECTED_END_AFTER_WHERE = "Expected criteria after 'WHERE', but statement ended prematurely.";
  public static final String EXPECTED_WHERE_FOUND_PARAM1 = "Expected WHERE. Found {0}";
  public static final String INSERT_ROW = "The row could not be inserted.";
  public static final String UPDATE_ROW = "The row could not be updated.";
  public static final String DELETE_ROW = "The row could not be deleted.";
  public static final String ENDED_BEFORE_COLNAME_FOUND = "Expected column name after table name, but statement ended prematurely.";
  public static final String ENDED_BEFORE_COLNAME_PARSED = "Unexpected end of statement while parsing column name.";
  public static final String EXPECTED_GROUP = "Expected GROUP. Found: {0}";
  public static final String EXPECTED_HAVING = "Expected HAVING. Found: {0}";
  public static final String EXPECTED_NUM_FOUND_PARAM1 = "Expected a number or ','. Found '{0}' instead.";
  public static final String EXPECTED_PARAM1_FOUND_PARAM2 = "Expected {0}. Found: {1}";
  public static final String EXPECTED_COLNAME_FOUND_PARAM1 = "Expected a column name or ','. Found '{0}' instead.";
  public static final String EXPECTED_ORDER = "Expected ORDER. Found: {0}";
  public static final String EXPECTED_FIRST_LAST_AFTER_NULLS = "Expected FIRST or LAST after NULLS";
  public static final String MISSING_CLOSING_PAREN = "Missing closing parenthesis.";
  public static final String SYNTAX = "Syntax error near [{0}]";
  public static final String EXPECTED_FROM_FOUND_PARAM1 = "Expected FROM. Found: {0}";
  public static final String PARSE_DOUBLE = "Error parsing double value [{0}].";
  public static final String INVALID_INT = "The value {0} is not a valid integer.";
  public static final String EMPTY_STATEMENT = "Empty statement.";
  public static final String EXPECTED_CLOSING_PAREN = "Expected ')' but statement ended prematurely.";
  public static final String VALUE_CLAUSE_UNMATCHED = "VALUE clause has more values than the column list.";
  public static final String ENDED_BEFORE_EXPECTED_VALUE = "Expected value but statement ended prematurely.";
  public static final String EXPECTED_VALUES = "Expected 'VALUES'. Found: {0}";
  public static final String EXPECTED_START_PAREN = "Expected '('. Found: {0}";
  public static final String INSERT_SELECT_STAR = "Column name '*' cannot be used for INSERT-SELECT query.";
  public static final String EXPECTED_TABLENAME_BEFORE_WHERE = "Expected table name before WHERE keyword.";
  public static final String EXPECTED_EQUALS = "Expected '=' but found '{0}'.";
  public static final String ENDED_BEFORE_PARAMNAME_VALUE = "Expected parameter=value expression but statement ended prematurely.";
  public static final String ENDED_BEFORE_TABLENAME = "Expected table name but statement ended prematurely.";
  public static final String ENDED_BEFORE_DATATYPE = "Expected data type but statement ended prematurely.";
  public static final String ENDED_BEFORE_COLSIZE = "Expecting column size but statement ended prematurely.";
  public static final String ENDED_BEFORE_COLNAME = "Expecting column name but statement ended prematurely.";
  public static final String NO_SELECT_WITH_EXECUTEUPDATE = "executeUpdate does not accept SELECT statements. Use executeQuery instead.";
  public static final String USE_EXECUTEQUERY_WITH_SP = "Use executeQuery to execute stored procedures.";
  public static final String EXECUTEQUERY_ACCEPTS_SELECT_ONLY = "executeQuery only accepts the SELECT statement. Use executeUpdate instead.";
  public static final String ENDED_BEFORE_COLNAME_VALUE = "Expected column=value expression but statement ended prematurely.";
  public static final String ENDED_BEFORE_PARAM1 = "Expected {0} but statement ended prematurely.";
  public static final String EXPECTED_IDENTIFIER = "Expected identifier but found {0} with value '{1}' instead at position {2}.";
  public static final String EXPECTED_PRIMARY_KEY = "Expected primary key but statement ended prematurely.";
  public static final String UPDATE_SELECT_STAR = "Column name '*' cannot be used for UPDATE-SELECT query.";
  public static final String SQL_COLCOUNT = "SQL column count does not match.";
  public static final String MALFORMED_SQL = "Malformed SQL Statement: {0}\r\nStatement:{1}";
  public static final String INPUT_READ_ERROR = "End of input text was reached.";
  public static final String EXPECTED_ENDQUOTE = "Could not find expected closing quote character {0} matching the one found at position {1}";
  public static final String UNKNOWN_TOKEN = "Unknown token {0} encountered during parsing at position {1}";
  public static final String EXPECTED_TOKEN = "Expected token '{0}' but found {1} with value '{2}' instead at position {3}.";
  public static final String NO_COL_DEF = "No column definition [{0}].";
  public static final String ENDED_BEFORE_COL_DEF = "Expected column definition but statement ended prematurely.";
  public static final String SYNTAX_INT = "Syntax error near [{0}]. Expected an integer value.";
  public static final String UNSUPPORTED_CONSTRAINT = "Constraint is not supported yet. Only the primary key can be used.";
  public static final String UNEXPECTED_TOKEN = "Unexpected token encountered: [{0}].";
  public static final String INVALID_DATA_TYPE_WITH_PARAMS = "The value [{0}] is not valid as a data type.";
  public static final String EXPECTED_ANON_PARAM = "Expected '?' but found: {0}.";
  public static final String ENDED_BEFORE_CLOSING_PAREN = "Statement ended prematurely before ')'.";
  public static final String ENDED_BEFORE_QUERY = "Expected a query but the statement ended prematurely.";
  public static final String ENDED_BEFORE_SP_NAME = "Expected stored procedure name but statement ended prematurely.";
  public static final String EXPECTED_SELECT_OR_GETDELETED = "Expected next token as SELECT or GETDELETED but found [{0}]";
  public static final String REQUIRES_INT_PARAM = "You must provide an integer. Provided [{0}].";
  public static final String REQUIRES_ONE_PARAM = "You must set one parameter.";
  public static final String CREATESCHEMA = "No procedure to create the schema was found.";
  public static final String NEW_CHANGESET = "Cannot begin a new changeset before completing the previous one.";
  public static final String CONNECTION_STRING_MODIFY_OPEN = "The connection string cannot be modified after the connection has been opened.";
  public static final String SELECT_STAR = "'*' must be the only column in the query when used.";
  public static final String COLUMN_NOT_FOUND = "The column '{0}' was not found.";
  public static final String COLUMN_DOES_NOT_EXIST = "The column [{0}] does not exist";
  public static final String INVALID_STRING_OPERATOR = "The operator '{0}' cannot be used on string columns. Column: [{1}].";
  public static final String CONNECTION_SUCCESSFUL = "The connection test was successful.";
  public static final String JAVA_PARAMETER_EXCEPTION = "Invalid parameter index: {0}. Index must be greater than 0 and no more than the number of parameters specified in the query.";
  public static final String INVALID_CACHE_OPTION = "Invalid cache option {0}. This is only available with REPLICATE.";
  public static final String NO_SUPPORT_OAUTH_NOPROMPT = "OAuth authentication is not supported.";
  public static final String CURSOR_MOVE_PRIVIOUS_UNSUPPORTED = "Moving previous is not supported by current cursor.";
  public static final String NOT_FOUND_DIALECT_SQL_BUILDER = "The '{0}' Dialect SqlBuilder can't be found.";
  public static final String NO_SUPPORT_SQL_NORMALIZATION = "Normalization is not supported for the SQL statement '{0}'.";
  public static final String NO_TABLE_REFERENCE_INCLUDE_IN_FROM = "No table found with the alias '{0}'.";
  public static final String NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR = "Deleting the left or right expression of SqlCondition is not supported.";
  public static final String NO_SUPPORT_EQUAL_IN_DATELITERALS = "Equals is not supported in date literals.";
  public static final String INVALID_PARAMETER_IN_LITERAL = "Invalid parameter in literal '{0}'";
  public static final String UNKNOWN_COLUMN_IN_FIELDS_LIST = "Unknown column '{0}'.";
  public static final String AMBIGUOUS_COLUMN_IN_FIELDS_LIST = "Column '{0}' is ambiguous.";
  public static final String INVALID_TYPE_FORMATTER = "Invalid data type formatter.";
  public static final String CROSS_APPLY_UNKNOWN_TABLE = "Column reference in CROSS APPLY clause references unknown table with prefix '{0}'.";
  public static final String CROSS_APPLY_INVALID_TVF = "CROSS APPLY clause is followed by unsupported table-valued function '{0}'.";
  public static final String CROSS_APPLY_NO_COLUMNS = "Table-valued function requires '{0}' at least 1 column definition in the WITH clause.";
  public static final String CROSS_APPLY_NO_COLUMN_REFERENCE = "Table-valued function '{0}' requires a column reference as the first argument.";
  public static final String CROSS_APPLY_INVALID_EXPRESSION = "Argument '{0}' to table-valued function '{1}' must be a constant expression.";
  public static final String CROSS_APPLY_DELIMITED_TABLE_TOO_MANY_COLUMNS = "DELIMITEDTABLE expression only supports a single column definition.";
  public static final String WARN_STRING = "Warning: {0}";

  public static SqlExceptions INSTANCE = new SqlExceptions();
  private Hashtable<String,String> j = null;
  protected  SqlExceptions() {
    j = new Hashtable<String,String>();
    // License Exceptions
    j.put("This system contains a license for {0} that has been installed but not activated.  You must run setup in order to activate the license on this system [code: {3} nodeid: {4}].",
    "このシステム上には、{0}用のライセンスがインストールされていますが、ライセンス認証されていない状況です。ライセンス認証を行うには、セットアップを実行する必要があります [コードcode：{3} nodeid：{4}]。");
    j.put("This system contains a trial license for {0} that has expired.  Please visit {1} or email {2} for information on purchasing a license or extending your trial [code: {3} nodeid: {4}].",
    "このシステム上には、有効期間が切れている{0}用の試用ライセンスがインストールされています。製品ライセンスの購入、または試用ライセンスの有効期間を延長するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。");
    j.put("Could not find a valid license for using {0} on this system.  To obtain a trial license, please visit {1} or email {2} [code: {3} nodeid: {4}].",
    "このシステム上に{0}を使用するための有効なライセンスが見つかりませんでした。試用ライセンスを入手するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。");
    j.put("Error reading registry: {0}",
    "レジストリを読み込み中にエラーが発生しました。{0}");
    j.put("Could not find a valid license for using {0} on this system.  To obtain a trial license, please visit {1} or email {2} [code: {3} nodeid: {4}].If you believe you already have a license, please check the following path(s):{5}, {6}.",
    "このシステム上に{0}を使用するための有効なライセンスが見つかりませんでした。試用ライセンスを入手するには、{2}までお問い合わせください [コード：{3} nodeid：{4}]。既にライセンスを入手している場合は、{5}、{6}のパスをご確認ください。");
    j.put("Invalid product key [code: {0} nodeid: {1}].",
    "無効なプロダクトキー[コード：{0} nodeid：{1}]。");   
    j.put("You must enter a valid Product Key in order to continue.",
    "続行するには、有効なプロダクトキーを入力する必要があります。"); 
    j.put("Contact {0} for further help.",
    "詳細は{0}までお問い合わせください。");
    j.put("Error downloading license: {0}",
    "ライセンスをダウンロード中にエラーが発生しました。{0}");
    j.put("\r\nPlease try to activate manually by going to:",
    "\r\n手動でライセンス認証を行う："); 
    j.put("Could not find {0}.jdbc.{1}.jar.",
    "{0}.jdbc.{1}.jarが見つかりません。");
    j.put("Error validating user input: {0}",
    "ユーザー入力の検証中にエラーが発生しました。{0}");
    j.put("License verification failed [code: {0}]. Please try again.",
    "ライセンスの検証に失敗しました [コード：{0}]。ライセンス認証を再度行ってください。");
    j.put("The license check failed for the following reason: {0}, code: {1}",
    "ライセンス検証失敗の原因：{0}、コード：{1}"); 
    j.put("Failed to get system version information. [code: {0} nodeid: {1}]",
    "システムバージョン情報の取得に失敗しました。[コード：{0} nodeid：{1}]"); 
    j.put("This application uses an evaluation version of the {0}. You may test it for a period of 30 days as specified in the Licensing Agreement. Further use requires a license. For more information, please visit {2}.",
    "{0}({1} component)。\r\nこのアプリケーションは、コンポーネントの評価版を使用しています。ライセンス使用許諾契約書により、30日間を通じて評価版を使用できます。ご使用を続行したい場合、製品ライセンスをご購入ください。詳細については、{2}をご覧ください。");
    j.put("This license can not be used in a Server Operating System [code: {1} nodeid: {2}]",
    "このライセンスは、サーバー・オペレーティング・システム上で使用できません[コード：{1} nodeid：{2}]");
    j.put("The beta version of {0} used by this application has expired.\nPlease visit {1} for an updated version.",
    "このアプリケーションで使用している{0}のベータ版の有効期間が切れました。\n更新版については、{1}をご覧ください。");
    j.put("License installation succeeded.",
    "ライセンスのインストールは成功しました。");
   // Core Provider Exceptions
    j.put("Incorrect value for the connection property Max Precision. It requires an integer. The following value was provided: {0}.",
    "接続プロパティの最大有効桁数の値が不正確です。整数値が必須です。設定された値は{0}です。");
    j.put("Could not execute the specified command: {0}",
    "指定したコマンドが実行できませんでした。{0}");

    //ado
    j.put(EF_SQLGEN_STATEMENT,
    "{0}ステートメント用のSQL生成はまだサポートされていません。");
    j.put(EF_COMMANDTREE_WITH_PARAMS,
    "型{0}のコマンドツリー用のSQLを生成中にエラーが発生しました。\r\n 例外：{1}");
    j.put(EF_NONPRIMITIVE_TYPE,
    "プリミティブ型以外のパラメータを作成できません。");
    j.put(EF_SQLGEN_EXPRESSION,
    "{0}式用のSQL生成はまだサポートされていません。");
    j.put(SCAN_DEFININGQUERY,
    "DefiningQueryに対応されたScan式はサポートされていません。");
    j.put(EF_SQLGEN_NEW_INSTANCE,
    "行型のないNewInstance式はサポートされていません。");
    j.put(EF_SQLGEN_UNSUPPORTED_RESULT_TYPE,
    "入れ子になったレコード型または複合型オブジェクトはサポートされていません。");
    j.put(EF_SQLGEN_NONCONSTANT_VALUE_IN_LIMIT,
    "Limit式は定数値のみでサポートされています。");
    j.put(LINQ_NO_TABLE_ATTRIBUTE,
    "{0}はTableAttributeで修飾されていません。");
    j.put(LINQ_NO_TABLE_FOR_TYPE,
    "型{0}のテーブルを取得できませんでした。");
    j.put(EF_UNKNOWN_PRIMITIVE_TYPE,
    "不明なPrimitiveTypeKind {0}");
    j.put(EF_UNSUPPORTED_INFORMATION_TYPE,
    "プロバイダは、informationType'{0}'をサポートしていません。");
    j.put(EF_UNSUPPORTED_TYPE,
    "プロバイダは、type'{0}'をサポートしていません。");
    j.put(EF_NO_STORE_TYPE_FOR_EDM_TYPE,
    "プリミティブ型'{1}'のEDM型'{0}'に対応するストア型はありません。");
    j.put(EF_MANIFEST,
    "コマンドが作成できませんでした：DbProviderManifest の入力値は nullです。");
    j.put(EF_SQLGEN_SELECT,
    "SELECTステートメント用のSQL生成は許可されていません。");  
    j.put(EF_COMMANDTREE,
    "コマンドが作成できませんでした：DbCommandTree の入力値は nullです。");
    j.put(LINQ_NOT_MARKED_AS_TABLE,
    "{0}がTableとしてマークされていません。");
    j.put(LINQ_NULL_EXPRESSION_TREE,
    "クエリが作成できませんでした：式ツリーの値は null です。");
    j.put(LINQ_NO_QUERYABLE_ARGUMENT,
    "クエリが作成できませんでした：クエリ可能な引数が必要です。"); 
    j.put(LINQ_NO_MATCHING_ENTITY,
    "指定された基準に一致するエンティティはありません。");
    j.put(LINQ_UNSUPPORTED_EXPRESSION,
    "このクエリ式はサポートされていません。");
    j.put(EF_UNSUPPORTED_OPERATOR,
    "演算子{0}はサポートされていません。");

   
    //linq
    j.put(LINQ_NO_ITEMS_FOR_PROJECTION, 
    "このクエリには、射影するアイテムが含まれていません！");
    j.put(LINQ_NO_MULTIPLE_RESULT_SHAPES,
    "複数の結果形状がサポートされていません。");
    j.put(LINQ_NOT_QUERYABLE,
    "無効なシーケンス演算子の呼び出しです。演算子の型はクエリ可能ではありません！");
    j.put(LINQ_MULTIPLE_WHERE_CLAUSES,
    "式には、2つ以上のWhere句を使用することができません。");
    j.put(LINQ_MULTIPLE_SELECT,
    "式には、2つ以上のSelectステートメントを使用することができません。");
    j.put(LINQ_MULTITAKE_VALUE,
    "式には、2つ以上のTake/First/FirstOrDefaultを使用することができません。");
    j.put(LINQ_UNARY,
    "単項演算子'{0}'がサポートされていません。");
    j.put(LINQ_BINARY_WHERE,
    "Where述語は'{0}'二項演算子をサポートしません。");
    j.put(LINQ_WHERE_CLASS,
    "Where式内のメンバー{0}は、{1}クラス上に見つかりません。");
    j.put(LINQ_SUBQUERIES,
    "サブクエリがサポートされていません。");
    j.put(LINQ_NONMEMBER,
    "メンバー'{0}'がサポートされていません。");
    j.put(LINQ_NULL_METATABLE,
    "{0}のメタデータを取得できませんでした。");
    j.put(LINQ_NULL_METAMEMBER,
    "Select式内のメンバー{0}は、{1}クラス上に見つかりません。または選択可能ではありません。");
    j.put(LINQ_UNSUPPORTED_MEMBER_TYPE,
    "メンバー型がサポートされていません。");
    j.put(LINQ_UNSUPPORTED_CONSTANT,
    "'{0}'用の定数がサポートされていません。");
    j.put(LINQ_NO_MAPPING,
    "{0}用のマップ列を検索できませんでした。");
    j.put(LINQ_BAD_STORAGE_PROPERTY,
    "BadStorageProperty");
    j.put(LINQ_UNHANDLED_EXPRESSION_TYPE,
    "ハンドルされていない式の型：'{0}'");
    j.put(LINQ_UNHANDLED_BINDING_TYPE,
    "ハンドルされていない連結の型：'{0}'");
    j.put(EF_NOT_EXPRESSIONS_UNSUPPORTED,
    "複合型式上のNot式がサポートされていません。");
    //ssr
    j.put(SSRS_COMMAND_TYPE_ONLY,
    "この接続はCommandType.Textのみをサポートします。");
    j.put(SSRS_TRANSACTIONS,
    "トランザクションがサポートされていません。");
    j.put(SSRS_UNSUPPORTED_IMPERSONATION,
    "{0} SSRS Provider for {1}は、偽装をサポートしません。");
    j.put(SSRS_INTEGRATED_SECURITY,
    "このプロバイダは、統合セキュリティをサポートしません。");
    j.put(SSRS_USERPASS,
    "このプロバイダは、ユーザー名・パスワードを使用する認証処理をサポートしません。");
    j.put(CANNOT_SET_PROVIDERNAME,
    "ProviderNameプロパティを設定できません。");
    j.put(NULL_DATAREADER,
    "値 ");
    j.put(NULL_DATAREADER_BUFFER,
    "バッファ");
    j.put(HASROWS_NOT_SUPPORTED,
    "現時点では、HasRowsがサポートされていません。");
    j.put(LINQ_NULL_QUERY,
    "クエリ");
    j.put(KEYWORD_NOT_SUPPORTED,
    "キーワードがサポートされていません。'{0}'");
      
    //xls
    j.put(XLS_APPLICATION_SETTINGS,
    "このプロバイダのためのアプリケーション設定はありません。");
    j.put(XLS_TRIAL_INSERT,
    "評価版では、行の挿入機能が提供されません。挿入機能を有効にするには、製品ライセンスをご購入ください。");
    j.put(XLS_TRIAL_DELETE,
    "評価版では、行の削除機能が提供されません。削除機能を有効にするには、製品ライセンスをご購入ください。");
    j.put(XLS_TRIAL_UPDATE,
    "評価版では、行の更新機能が提供されません。更新機能を有効にするには、製品ライセンスをご購入ください。");
    j.put(XLS_CONN_NOT_OPEN,
    "テストする前に接続を開いておく必要があります。");
    j.put(XLS_INVALID_LICENSE,
    "本製品のライセンスは有効期限が切れたか、無効です。");
    j.put("{0} Excel Add-In",
    "{0} Excel アドイン");
    j.put(PROPERTY_MISSING,
    "{0} プロパティは初期化されていません。");
    
    //CacheProvider
    j.put(CACHE_EMPTY_CONNECTION_STRING,
    "[{0}]：接続文字列を空にすることはできません。");
    j.put(CACHE_CANNOT_LOAD_DRIVER,
    "適当なドライバをロードできません：[{0}]。詳細情報：{1}。");
    j.put(CACHE_CANNOT_CONNECT,
    "プロバイダを使用して接続を確立できません：[{0}]。詳細情報：{1}。");
    j.put(CACHE_DBPROVIDERFACTORIES_XAMARIN,
    "DbProviderFactoriesはXamarinに存在しません。");
    j.put(CACHE_RANGE,
    "値[{0}]が予想範囲内に収まりません。");
    j.put(NULL_CONNECTION_STRING,
    "接続文字列をnullにすることはできません。");
    j.put(CACHE_RETRIEVE_METADATA,
    "データベースのメタデータを取得できません。");
    j.put(NO_PREPARED_STATEMENT,
    "実行対象のプリペアドステートメントはありません。");
    j.put(JAVADB_NULL,
    "JavaDbが開いていないため、コマンドを実行できません。");
    j.put(JAVADB_CLOSED,
    "JavaDbが閉じているため、コマンドを実行できません。");
    j.put(NO_RESULT_SET,
    "現時点では、利用可能な結果セットはありません。");
    j.put(CACHEDRIVER,
    "CacheDriverが指定されていません。CacheDriverを構成してください。");
    j.put(CACHECONNECTION,
    "CacheConnectionが指定されていません。CacheConnectionを構成してください。");
    j.put(CACHING_IN_PROGRESS,
    "キャッシュ処理は実行中です。現在の処理が完了する前にキャッシュできません。");
    j.put(CONNECTION_CLOSED,
    "最初に、接続する必要があります。");
    j.put(SCHEMA_MODIFIED,
    "データをキャッシュできません。テーブルのスキーマが変更されています。");
    j.put(CANNOT_ALTER_SCHEMA,
    "テーブルスキーマを変更できません。エラー：{0}");
    j.put(CHANGESET_INACTIVE,
    "チェンジセットがアクティブである場合のみ、キャッシュが変更できます。");
    j.put(CACHE_PRIMARY_KEYS_REQUIRED,
    "キャッシュされた行を更新するには、主キーが必要です。");
    j.put(CACHE_PRIMARY_KEYS_NOT_DISCOVERED,
    "内部エラー：利用可能な主キーはありません。主キーをあらかじめ取得しておく必要があります。");
    j.put(INVALID_DATA_TYPE,
    "無効なデータ型。");
    j.put(INVALID_COL_DATA_TYPE,
    "無効な列データ型[{0}]。");
    j.put(UNSUPPORTED_CULTURE_SETTING,
    "指定されたカルチャー設定[{0}]はサポートされていません。");
    j.put(INVALID_CONNECTION_STRING,
    "入力された接続文字列が無効です。");
    j.put(INVALID_CONNECTION_STRING_SYNTAX,
    "接続文字列の構文とインデックスが無効です：{0}。");
    j.put(INVALID_CONNECTION_STRING_KEY,
    "接続文字列のキー名 [{0}]が無効です。");
    j.put(NULL_POINTER_EXCEPTION,
    "Nullポインタの例外");
    j.put(NOT_A_BOOLEAN,
    "{0}はブール値ではありません。");
    j.put(RESULTSET_NOT_READY,
    "結果セットが準備できていません。");
    j.put(INVALID_OUTPUT_PARAMETER,
    "インデックス[{0}]のパラメータは出力パラメータではありません。");
    j.put(MAX_PARAMETERS_EXCEEDED,
    "プロシージャには、({0})以上のパラメータを設定することはできません。"); 
    j.put(NO_PARAMETER_FOUND,
    "[{0}]というパラメータが見つかりません。");
    j.put(UNSUPPORTED_FEATURE,
    "この機能はサポートされていません。");
    j.put(BAD_NUMBER_FORMAT,
    "数値形式が正しくありません：[{0}]。");
    j.put(METADATA_INDEX_OUT_OF_RANGE,
    "ParameterMetaData：インデックス[{0}]は範囲外です。");    
    j.put(READONLY_DATASOURCE,
    "読み取り専用のデータソースはSELECTクエリのみをサポートします。");
    j.put(CREATE_TABLE,
    "クエリが実行できません。[CREATE TABLE]がサポートされていません。");
    j.put(TABLE_ALREADY_EXISTS,
    "テーブル[{0}]が既に存在しています。");
    j.put(DROP_TABLE_UNSUPPORTED,
    "クエリが実行できません。[DROP TABLE]がサポートされていません。");
    j.put(SQLPARSER_NULL,
    "このステートメントは指定されたコマンド型に対して無効です。");
    j.put(CANNOT_BIND_PARAMETER,
    "パラメータが連結できません：{0}。");
    j.put(UPDATE_READONLY_COLUMN,
    "[{0}]列は読み取り専用です。この列の値を更新できません。");
    j.put(NUMERIC_COL_DATATYPE_INVALID,
    "小数点以下桁数と有効桁数の値はnumeric型であることが必要です。ご使用のスキーマを再確認してください。");
    j.put(IGNORETYPES_INVALID,
    "IgnoreTypes接続プロパティの値が正しくありません。型のカンマ区切りの文字列が必須です。設定された値は{0}です。");
    j.put(LOAD_RUNTIME_SETTINGS_FAILED,
    "ランタイム設定のロード中に失敗しました。指定されたファイルが存在しません。ファイル：{0}。");
    j.put(LOCATION_DOES_NOT_EXIST,
    "接続文字列に指定されたファイルまたはフォルダが存在しません。場所 = {0}");
    j.put(NO_TEST_CONNECTION,
    "このプロバイダは、テスト接続をサポートしません。");
    j.put(INVALID_OBJECT_NAME,
            "オブジェクト名'{0}'が無効です。");
    j.put(INVALID_COLUMN_NAME,
            "INVALID_COLUMN_NAME");
    j.put(CANNOT_GET_INFO_ITEM,
    "情報アイテムを取得できません。");
    j.put(INVALID_METHOD,
    "スクリプトに無効な{0}メソッドが見つかりました。");
    j.put(CANNOT_RETRIEVE_COLUMNS,
    "[{0}]テーブルの列が取得できません。");
    j.put(REFERENCE_MISSING_TABLE_AND_COLUMN,
    "参照({0})にはtablenameとcolnameが含まれていません。");
      j.put(CLOSE_TEMPDB,
    "一時データベースを閉じる操作に失敗しました。エラー：{0}");
    j.put(CREATE_TEMPDB,
    "一時データベースの作成に失敗しました。エラー：{0}");
    j.put(QUERYCACHE_WITH_NONSQLITE_DRIVER,
    "接続を確立できません。QueryCacheは[{0}]のみで使用できます。");
    j.put(NO_CACHE_CONNECTION,
    "キャッシュ接続を設定する必要があります。");
    j.put(CONNECTION_QUERYCACHE_INT,
    "QueryCacheに数値を設定する必要があります。");
    j.put(CONNECTION_SCHEMACACHEDURATION_INT,
    "SchemaCacheに数値を設定する必要があります。");
    j.put(MAXCONNECTIONS_INT,
    "MaxConnectionsに整数値を設定する必要があります。");
    j.put(CACHE_DB_TYPE_UNRECOGNIZED,
    "不明な'Cache DB Type'値が見つかりました：[{0}]。使用可能な値：'MySQL'、'SQLServer'、'Oracle'、'Generic'");
    j.put(INDEX_OUT_OF_RANGE,
    "インデックスは範囲外です。インデックスの範囲は1から{0}までです。");
    j.put(PARAM1_CANNOT_BE_FOUND_IN_METADATADB,
    "'{0}'テーブルは、メタデータのデータベースに見つかりません。");
    j.put(ONE_READMODE,
    "指定できる値は１つのみです。利用可能な値はReadAsNullおよびReadAsEmptyです。");
    j.put(INVALID_GETUPDATEBITS,
    "指定できる値は１つのみです。利用可能な値はUpdateToNull、UpdateToEmpty、およびIgnoreInUpdateです。");     
    j.put(INVALID_GETINSERTBITS,
    "指定できる値は１つのみです。利用可能な値はInsertAsNull、InsertAsEmpty、およびIgnoreInInsertです。");
    j.put(CONNECTION_MODE_INVALID,
    "不明なモードが検出されました：[{0}]");
    j.put(CACHEPARTIAL_FALSE,
    "自動キャッシュは、すべての列（射影ではない）をキャッシュするクエリの場合のみ使用できます。部分的にキャッシュするには、CachePartial=Trueを設定する必要があります。");
    j.put(UNABLE_TO_LOAD_PARAM1,
    "要求されたデータをロードできません。詳細情報：{0}");
    j.put(EXEC_CACHE_NULL,
    "キャッシュが構成されていない場合、キャッシュからコマンドを実行できません。");
    j.put(INVALID_QUERY,
    "無効なクエリ[{0}]。");
    j.put(MULTIPLE_SYNC_BLOCKS,
    "複数の同期ブロックが検出されましたが、EnableMultipleSyncsはFalseに設定されています。");
    j.put(TRANSACTION_NOT_SUPPORTED,
    "トランザクションがサポートされていません。");
    j.put(BULKOPERATION_NOT_SUPPORTED,
    "バルク処理はサポートされていません。");
    j.put(MIXBULKOPERATION_NOT_SUPPORTED ,
    "バルク処理タイプの混合はサポートされていません。");
    j.put(UPDATEGRAMS_ERROR,
    "アップデートグラムエラー：{0}");
    j.put(UPDATE_VIEW_FAILED,
    "ビューを更新できません。");
    j.put(UNSUPPORTED_CACHE_QUERY,
    "サポートされていないキャッシュクエリ：{0}");
    j.put(MISSING_TRANSACTION_ID,
    "トランザクションIDが不足しています。");
    j.put(TRANSACTION_ALREADY_OPEN,
    "トランザクション[{0}]が既に開いています。");
    j.put(TRANSACTION_DOES_NOT_EXIST,
    "トランザクション[{0}]が存在しません。");
    j.put(TEMPDB_CREATE_TABLE,
            "一時データベースにテーブルを作成できません。詳細情報：{0}");
    j.put(NO_SCHEMA,
            "スキーマを取得できません。");
    j.put(NONTEXTCOMMAND_IN_CACHE,
    "キャッシュにテキスト以外のコマンドを実行しています。");  
    j.put(UNKNOWN_FUNCTION,
    "不明な関数が検出されました。関数：'{0}'。");
    j.put(UNKNOWN_DATATYPE,
    "不明なデータ型[{0}]。");
    j.put(INVALID_COLUMN_INDEX,
    "列のインデックス[{0}]が無効です。");
    j.put(TABLE_COLUMNS,
    "テーブルの列を取得できません。{0}");
    j.put(TABLE_METADATA,
    "テーブルのメータデータを取得できません。エラー：{0}");
    j.put(GETARRAY_INT,
    "getArray(int i)がサポートされていません。");
    j.put(GETARRAY_STRING,
    "getArray(String colName)がサポートされていません。");
    j.put(GETASCIISTREAM_INT,
    "getAsciiStream(int columnIndex)がサポートされていません。");
    j.put(GETASCIISTREAM_STRING,
    "getAsciiStream(String columnName)がサポートされていません。");
    j.put(RESULTSET_CURSOR,
    "結果セットを反復処理し、読み込む前に次の行を確認する必要があります。");
    j.put(GETBINARYSTREAM_INT,
    "getBinaryStream(int columnIndex)がサポートされていません。");
    j.put(GETBINARYSTREAM_STRING,
    "getBinaryStream(String columnName)がサポートされていません。");
    j.put(GETBLOB_INT,
    "getBlob(int i)がサポートされていません。");
    j.put(GETBLOB_STRING,
    "getBlob(String colName)がサポートされていません。");
    j.put(GETCHARACTERSTREAM_INT,
    "getCharacterStream(int columnIndex)がサポートされていません。");
    j.put(GETFETCHDIRECTION,
    "getFetchDirection()がサポートされていません。");
    j.put(GETFETCHSIZE,
    "getFetchSize()がサポートされていません。");
    j.put(GETOBJECT_INT,
    "getObject(int i, Map<String, Class<?> map)がサポートされていません。");
    j.put(GETOBJECT_STRING,
    "getObject(String colName, Map<String, Class<?> map)がサポートされていません。");
    j.put(GETREF_INT,
    "getRef(int i)がサポートされていません。");
    j.put(GETREF_STRING,
    "getRef(String colName)がサポートされていません。");
    j.put(GETUNICODESTREAM_STRING,
    "getUnicodeStream(String columnName)がサポートされていません。");
    j.put(GETCLOB_INT,
    "getClob(int i)がサポートされていません。");
    j.put(GETCLOB_STRING,
    "getClob(String colName)がサポートされていません。");
    j.put(GETURL_INT,
    "getURL(int columnIndex)がサポートされていません。");
    j.put(GETURL_STRING,
    "getURL(String columnName)がサポートされていません。");
    j.put(GETUNICODESTREAM_INT,
    "getUnicodeStream(int columnIndex)がサポートされていません。");
    j.put(METADATA_TABLES_READONLY,
    "メタデータテーブルではSELECTクエリのみ実行できます。");
    j.put(PARAMETER_NOT_FOUND,
    "パラメータコレクションには、パラメータ{0}が見つかりませんでした。");
    j.put(UNRECOGNIZED_COLUMN,
    "テーブル [{1}] のクエリで列 [{0}] が認識されません。");
    j.put(OAUTHSETTINGS_READ,
    "OAuthSettingsの読み込み中にエラーが発生しました：[{0}]");
    j.put(OAUTHSETTINGS_WRITE,
    "OAuthSettingsをディスクに書き込み中にエラーが発生しました：[{0}]");
    j.put(INITIATEOAUTH,
    "OAuthを開始するには、'OAuthClientId'および'OAuthClientSecret'が必要です。");
    j.put(INVALID_STATEMENT,
    "無効なステートメント。");
    j.put(ARRAY_PARAM,
    "現時点では、配列型のパラメータがサポートされていません。");
    j.put(ASCIISTREAM,
    "現時点では、ASCIIストリームのパラメータがサポートされていません。");
    j.put(BIGDECIMAL,
    "現時点では、BigDecimal型のパラメータがサポートされていません。");
    j.put(GETCHARACTERSTREAM_STRING,
    "getCharacterStream(String columnName)がサポートされていません。");
    j.put(BINARYSTREAM,
    "現時点では、Binaryストリームのパラメータがサポートされていません。");
    j.put(BLOB,
    "現時点では、Blob型のパラメータがサポートされていません。");
    j.put(CHARACTERSTREAM,
    "現時点では、Characterストリームのパラメータがサポートされていません。");
    j.put(CLOB,
    "現時点では、Clob型のパラメータがサポートされていません。");
    j.put(REF_PARAM,
    "現時点では、Ref型のパラメータがサポートされていません。");
    j.put(UNICODESTREAM,
    "現時点では、Unicodeストリームのパラメータがサポートされていません。");
    j.put(CONNECTION_OPEN,
    "[{0}]接続を開くことができません。");
    j.put(INVALID_STRING_OPERATOR,
    "'{0}'演算子を文字列型の列に使用できません。列：[{1}]。");
    j.put(COLUMN_DOES_NOT_EXIST,
    "{0}列が存在しません。");
    j.put(COLUMN_NOT_FOUND,
    "'{0}'列が見つかりません。");
    j.put(SELECT_STAR,
    "'*'列を使用する場合、クエリ内の単一の列であることが必要です。");
    j.put(USE_EXECUTEQUERY_WITH_SP,
    "ストアドプロシージャを実行する場合、executeQueryを使用してください。");
    j.put(NO_SELECT_WITH_EXECUTEUPDATE,
    "executeUpdateがSELECTステートメントを受け取りません。代わりに、executeQueryを使用してください。");
    j.put(CONNECTION_STRING_MODIFY_OPEN,
    "接続が開いている状態では接続文字列を変更できません。");
    j.put(NEW_CHANGESET,
    "チェンジセットを完了する前に、新しいチェンジセットを開始できません。");
    j.put(CREATESCHEMA,
    "スキーマ作成に関する手続きが見つかりません。");
    j.put(REQUIRES_ONE_PARAM,
    "1つのパラメータを設定する必要があります。");
    j.put(REQUIRES_INT_PARAM,
    "整数を設定する必要があります。[{0}]が提供されています。");
    j.put(INVALID_DATETIME_WITH_PARAMS,
    "値は有効な日時ではありません。[{0}]が提供されています。");
    j.put(INVALID_DATEPART_WITH_PARAMS,
    "datepartの文字列が無効です。[{0}]。");
    j.put(EXPECTED_SELECT_OR_GETDELETED,
    "次のトークンはSELECTまたはGETDELETEDが必要ですが、[{0}]が見つかりました。");
    j.put(ENDED_BEFORE_SP_NAME,
    "ストアドプロシージャの名前が必要ですが、ステートメントが途中で終了しました。");
    j.put(ENDED_BEFORE_QUERY,
    "クエリが必要ですが、ステートメントが途中で終了しました。");
    j.put(ENDED_BEFORE_CLOSING_PAREN,
    "')'の前でステートメントが途中で終了しました。");
    j.put(EXPECTED_ANON_PARAM,
    "'?'が必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_COMMA,
    "','が必要ですが、{0}が見つかりました。");
    j.put(SYNTAX,
    "'{0}'の近くで構文エラーが発生しました。");
    j.put(INVALID_DATA_TYPE_WITH_PARAMS,
    "[{0}]は有効なデータ型ではありません。");
    j.put(UNEXPECTED_TOKEN,
    "予想外のトークンが見つかりました：[{0}]。");
    j.put(UNSUPPORTED_CONSTRAINT,
    "制約はまだサポートされていません。PRIMARY KEYのみが使用できます。");
    j.put(SYNTAX_INT,
    "[{0}]の近くで構文エラーが発生しました。整数値が必要です。");
    j.put(ENDED_BEFORE_COL_DEF,
    "列定義が必要ですが、ステートメントが途中で終了しました。");
    j.put(NO_COL_DEF,
    "[{0}]列定義が見つかりません。");
    j.put(EXPECTED_TOKEN,
    "{0}トークンが必要ですが、{2}値が設定されている{1}が見つかりました。");
    j.put(UNKNOWN_TOKEN,
    "{1}位置上の解析処理中に不明な{0}トークンが見つかりました。");
    j.put(EXPECTED_ENDQUOTE,
    "{1}位置にある引用符に対する終端の引用符{0}が見つかりませんでした。");
    j.put(INPUT_READ_ERROR,
    "入力完了後、入力されたテキストを読み込みました。");
    j.put(MALFORMED_SQL,
    "不正な形式のSQLステートメント：{0}\r\nステートメント：{1}");
    j.put(SQL_COLCOUNT,
    "SQL列は一致していません。");
    j.put(UPDATE_SELECT_STAR,
    "'*'列名はUPDATE-SELECTクエリに使用できません。");
    j.put(EXPECTED_PRIMARY_KEY,
    "主キーが必要ですが、ステートメントが途中で終了しました。");
    j.put(EXPECTED_IDENTIFIER,
    "識別子が必要ですが、{1}値が設定されている{0}が見つかりました。");
    j.put(ENDED_BEFORE_PARAM1,
    "{0}が必要ですが、ステートメントが途中で終了しました。");
    j.put(ENDED_BEFORE_TABLENAME,
    "テーブル名が必要ですが、ステートメントが途中で終了しました。");
    j.put(EXECUTEQUERY_ACCEPTS_SELECT_ONLY,
    "executeQueryがSELECTステートメントのみを受け取ります。代わりに、executeUpdateを使用してください。");
    j.put(ENDED_BEFORE_COLNAME_VALUE,
    "column=value式が必要ですが、ステートメントが途中で終了しました。");
    j.put(USE_EXECUTEQUERY_WITH_SP,
    "ストアドプロシージャを実行する場合、executeQueryを使用してください。");
    j.put(NO_SELECT_WITH_EXECUTEUPDATE,
    "executeUpdateがSELECTステートメントを受け取りません。代わりに、executeQueryを使用してください。");
    j.put(ENDED_BEFORE_COLNAME,
    "列名が必要ですが、ステートメントが途中で終了しました。");
    j.put(ENDED_BEFORE_DATATYPE,
    "データ型が必要ですが、ステートメントが途中で終了しました。");           
    j.put(ENDED_BEFORE_COLSIZE,
    "列のサイズが必要ですが、ステートメントが途中で終了しました。");
    j.put(ENDED_BEFORE_PARAMNAME_VALUE,
    "parameter=value式が必要ですが、ステートメントが途中で終了しました。");
    j.put(EXPECTED_EQUALS,
    "'='が必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_TABLENAME_BEFORE_WHERE,
    "WHEREキーワードの前にテーブル名が必要です。");
    j.put(INSERT_SELECT_STAR,
    "'*'列名はINSERT-SELECTクエリに使用できません。");
    j.put(EXPECTED_START_PAREN,
    "'('が必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_VALUES,
    "'VALUES'が必要ですが、{0}が見つかりました。");
    j.put(ENDED_BEFORE_EXPECTED_VALUE,
    "値が必要ですが、ステートメントが途中で終了しました。");
    j.put(VALUE_CLAUSE_UNMATCHED,
    "VALUE句には、列一覧よりも多くの値が含まれています。");
    j.put(EXPECTED_CLOSING_PAREN,
    "')'が必要ですが、ステートメントが途中で終了しました。");
    j.put(EMPTY_STATEMENT,
    "空のステートメントです。");
    j.put(INVALID_INT,
    "{0}は有効な整数値ではありません。");
    j.put(PARSE_DOUBLE,
    "[{0}]ダブル値の解析中にエラーが発生しました。");
    j.put(EXPECTED_FROM_FOUND_PARAM1,
    "FROMが必要ですが、{0}が見つかりました。");
    j.put(MISSING_CLOSING_PAREN,
    "右かっこがありません。");
    j.put(EXPECTED_ORDER,
    "ORDERが必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_FIRST_LAST_AFTER_NULLS,
    "NULLS の後にはFIRSTまたはLASTが必要です。");
    j.put(EXPECTED_COLNAME_FOUND_PARAM1,
    "列名または','が必要ですが、'{0}'が見つかりました。");
    j.put(EXPECTED_PARAM1_FOUND_PARAM2,
    "{0}が必要ですが、{1}が見つかりました。");
    j.put(EXPECTED_NUM_FOUND_PARAM1,
    "','数値が必要ですが、'{0}'が見つかりました。");
    j.put(EXPECTED_HAVING,
    "HAVINGが必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_GROUP,
    "GROUPが必要ですが、{0}が見つかりました。");
    j.put(EXPECTED_COLNAME_OR_PARAM1_FOUND_PARAM2,
    "-列名、','または'{0}'が必要ですが、'{1}'が見つかりました。");
    j.put(ENDED_BEFORE_COLNAME_PARSED,
    "列名を解析中にステートメントが予期せず終了しました。");
    j.put(ENDED_BEFORE_COLNAME_FOUND,
    "テーブル名の後に列名が必要ですが、ステートメントが途中で終了しました。");
    j.put(DELETE_ROW,
    "行を削除できませんでした。");
    j.put(UPDATE_ROW,
    "行を更新できませんでした。");
    j.put(INSERT_ROW,
    "行を挿入できませんでした。");    
    j.put(EXPECTED_WHERE_FOUND_PARAM1,
    "WHEREの代わりに{0}が見つかりました。");
    j.put(UNEXPECTED_END_AFTER_WHERE,
    "'WHERE'の後に基準が必要ですが、ステートメントが途中で終了しました。");
    j.put(UNMATCHED_CLOSING_PAREN,
    "')'に一致する'('が見つかりません。");
    j.put(UNMATCHED_OPENING_PAREN,
    "'('に一致する')'が見つかりません。");
    j.put(OPERATOR_CANNOT_BE_NEGATED,
    "{0}演算子を否定できません。");
    j.put(PAREN_LIST_FOLLOWS_PARAM1,
    "[{0}]の後は、かっこで囲まれる項目一覧が必要です。");
    j.put(UNEXPECTED_END,
    "基準を解析中にステートメントが予期せず終了しました。");
    j.put(UNEXPECTED_END_BEFORE_PARAM,
    "ステートメントが予期せず終了しました。パラメータが必要です。");
    j.put(UNEXPECTED_END_BEFORE_END_PAREN,
    "ステートメントが予期せず終了しました。右かっこが必要です。");
    j.put(EXPECTED_END_PAREN_FOUND_PARAM1,
    "右かっこが必要ですが、'{0}'が見つかりました。");
    j.put(EXPECTED_DATATYPE,
    "[{0}]の近くで構文エラーが発生しました。データ型が必要です。");
    j.put(EXPECTED_LEN,
    "[{0}]の近くで構文エラーが発生しました。長さが必要です。");
    j.put(EXPECTED_SCALE,
    "[{0}]の近くで構文エラーが発生しました。小数点以下桁数が必要です。");
    j.put(UNEXPECTED_DELIMITER,
    "予想外の区切り文字'{0}'が見つかりました。");
    j.put(PARSE_INT,
    "[{0}]整数値の解析中にエラーが発生しました。");
    j.put(NULL_ANALYZER,
    "アナライザー名はNULLです。");
    j.put(UNSUPPORTED_ANALYZER,
    "'{0}'アナライザーはサポートされていません。");  
    j.put(AT_LEAST_ONE_PARAM,
    "少なくとも1つのパラメータを設定する必要があります。");      
    j.put(TOO_MANY_OPEN_CONNECTIONS,
    "開いている接続が多すぎます。プロバイダは、同時接続数が{0}を超えないように構成されています。ご使用後は接続を閉じることにご注意ください。");
    j.put(EF_DESIGN_TIME_METADATA,
    "デザイン時の操作性を向上させるメタデータが作成できません。詳細情報：{0}");
    j.put(REQUIRES_THREE_PARAMS,
    "3つのパラメータを設定する必要があります。");
    j.put(INVALID_DATETIME_WITH_PARAMS,
    "値は有効な日時ではありません。[{0}]が提供されています。");
    j.put(YEAR_UNSUPPORTED,
    "年はまだサポートされていません。");
    j.put(QUARTER_UNSUPPORTED,
    "四半期はまだサポートされていません。");
    j.put(MONTH_UNSUPPORTED,
    "月はまだサポートされていません。");
    j.put(DAYOFYEAR_UNSUPPORTED,
    "年間積算日はまだサポートされていません。");
    j.put(WEEK_UNSUPPORTED,
    "週はまだサポートされていません。");
    j.put(WEEKDAY_UNSUPPORTED,
    "曜日はまだサポートされていません。");
    j.put(MICROSECOND_UNSUPPORTED,
    "マイクロ秒はまだサポートされていません。");
    j.put(NANOSECOND_UNSUPPORTED,
    "ナノ秒はまだサポートされていません。");
    j.put(INVALID_YEAR,
    "年の値が無効です。");
    j.put(INVALID_MONTH,
    "月の値が無効です。");
    j.put(INVALID_DAY,
    "日付の値が無効です。");
    j.put(TZOFFSET_UNSUPPORTED,
    "TZoffsetはまだサポートされていません。");
    j.put(NULL_DATEPART,
    "datepartの値をnullにすることはできません。");
    j.put(REQUIRES_TWO_PARAMS,
    "2つのパラメータを設定する必要があります。");
    j.put(INVALID_DATETIME,
    "datetimeの文字列が無効です！");
    j.put(INVALID_DATEPART,
    "datepartの文字列が無効です！");
    j.put(AT_LEAST_TWO_PARAMS,
    "少なくとも2つのパラメータを設定する必要があります。");
    j.put(AT_MOST_THREE_PARAMS,
    "最大で3つのパラメータを設定することができます。");
    j.put(DATEFIRST_INT_RANGE,
    "datefirstパラメータの値は、1～7間の整数値である必要があります。");
    j.put(INVALID_PRECISION,
    "有効桁数の値が無効です。");
    j.put(INVALID_FRACTION,
    "小数部分の値が無効です。");
    j.put(INVALID_SECONDS,
    "秒の値が無効です。");
    j.put(INVALID_MINUTE,
    "分の値が無効です。");
    j.put(INVALID_HOUR,
    "時間の値が無効です。");
    j.put(REQUIRES_EIGHT_PARAMETERS,
    "8つのパラメータを設定する必要があります。");
    j.put(INVALID_MILLISECONDS,
    "ミリ秒の値が無効です！");
    j.put(REQUIRES_SEVEN_PARAMETERS,
    "7つのパラメータを設定する必要があります。");
    j.put(MONTH_TO_ADD_INT,
    "month_to_addは整数値である必要があります。");
    j.put("tart_date is not a valid date.",
    "tart_dateは有効な日付ではありません。");
    j.put(INVALID_START_DATE,
    "start_dateは有効な日付ではありません。");
    j.put(AT_MOST_TWO_PARAMS,
    "最大で2つのパラメータを設定することができます。");
    j.put(POSITIVE_COUNT,
    "集計する値に正数を設定する必要があります。");
    j.put(CAST_TO_FLOAT,
    "[{0}]を浮動小数点数にキャストできません。");
    j.put(CAST_TO_INT,
    "[{0}]を整数にキャストできません。");
    j.put(CAST_TO_NUM,
    "[{0}]を数値にキャストできません。");
    j.put(REQUIRES_POSITIVE,
    "この引数に正数を設定する必要があります。[{0}]が提供されています。");
    j.put(POSITIVE_REPEAT,
    "繰り返して集計するために正数を設定する必要があります。");
    j.put(POSITIVE_CHAR_COUNT,
    "集計する文字の値に正数を設定する必要があります。");
    j.put(REQUIRES_FIVE_PARAMS,
    "5つのパラメータを設定する必要があります。");
    j.put(POSITIVE_DECIMAL,
    "小数のパラメータの値に正数を設定する必要があります。");
    j.put(POSITIVE_LEN,
    "長さの値に正数を設定する必要があります。");
    j.put(CONVERT,
    "変換する数値を設定する必要があります。");
    j.put(START_INT,
    "開始位置の値に整数を指定する必要があります。");
    j.put(LENGTH_INT,
    "長さの値に整数を指定する必要があります。");
    j.put(POSITIVE_LEN,
    "長さの値に正数を指定する必要があります。");
    j.put(NOARGS,
    "引数はありません。");
    j.put(AT_LEAST_TWO_PARAMS,
    "2つのパラメータを設定することができます。");
    j.put(REQUIRES_TWO_PARAMS,
    "2つのパラメータを設定する必要があります。");
    j.put(NO_SUCH_TABLE,
    "[{0}]テーブルが存在しません。");
    j.put(UNRECOGNIZED_KEYWORD,
    "不明なキーワード：{0}");
    j.put(JAVA_PARAMETER_EXCEPTION,
    "パラメータインデックスが無効です： {0}。インデックスは0より大きく、クエリで指定されたパラメータ数を超えないようにする必要があります。");
    j.put(INVALID_CACHE_OPTION,
    "キャッシュオプションが無効です {0}。これはREPLICATEでのみ利用可能です。");
    //SqlParser
    j.put(NOT_FOUND_DIALECT_SQL_BUILDER,
            "'{0}' dialect SqlBuilder が見つかりません。");
    j.put(NO_SUPPORT_SQL_NORMALIZATION,
            "SQL '{0}'の正規化はサポートされていません。");
    j.put(NO_TABLE_REFERENCE_INCLUDE_IN_FROM,
            "FROM に含める'{0}'のテーブル参照はありません。");
    j.put(NO_SUPPORT_SQL_MODIFICATION_DELETE_EXPR,
            "SqlConditionの左式または右式の削除はサポートされていません。");
    j.put(UNKNOWN_COLUMN_IN_FIELDS_LIST,
	        "不明なカラム'{0}'。");
    j.put(AMBIGUOUS_COLUMN_IN_FIELDS_LIST,
			"カラム'{0}'は不明です。");

    //ODBC
    j.put(ODBC_CANNOT_CONNECT,
    "サーバーに接続できません。構成を確認してサーバーへの接続をもう一度実行してください。");
    j.put(CONNECTION_SUCCESSFUL,
    "接続テストに成功しました。");
    j.put(CANNOT_OPEN_HELP,
    "ヘルプを開くことができません。");
    j.put(CANNOT_OPEN_HELP_WITH_PARAMS,
    "ヘルプ'{0}'を開くことができません。");
    j.put(INVALID_DSN,
    "データソース名'{0}'が無効です。ODBC Setup");
    j.put(NATIVE_CODE,
    "すべての関数は、ネイティブコードで返される必要があります。");
    //XLS
    j.put(NO_PROVIDER,
    "'{0}'というプロバイダはありません。");
    j.put(NO_UPDATE,
    "Updateステートメントを入力してください。");
    j.put(NO_INSERT,
    "Insertステートメントを入力してください。");
    j.put(NO_DELETE,
    "Deleteステートメントを入力してください。");
    j.put(NO_SELECT,
    "Selectステートメントを入力してください。");
    j.put(ARRAY_ARG,
    "引数は配列である必要があります。");
    j.put("Transaction has already been committed or is not pending",
    "トランザクションは既にコミットされているか、保留中ではありません。");
    j.put("Invalid parameter index: {0}. Index must be greater than 0 and no more than the number of parameters specified in the query.",
    "パラメータインデックスが無効です: {0}。インデックスは0より大きく、クエリで指定されたパラメータ数を超えないようにする必要があります。");
    j.put(NO_SUPPORT_OAUTH_NOPROMPT,
    "OAuth認証はサポートされていません。");
    j.put(CURSOR_MOVE_PRIVIOUS_UNSUPPORTED,
    "現在のカーソルでは前の移動はサポートされていません。");
    j.put(NO_SUPPORT_EQUAL_IN_DATELITERALS,
    "日付リテラルではイコールはサポートされていません。");
    j.put(INVALID_PARAMETER_IN_LITERAL,
            "リテラル '{0}'のパラメータが無効です。");
    j.put(INVALID_TYPE_FORMATTER,
            "データ型フォーマッタが無効です。");
    j.put(WARN_STRING,
            "Warning: {0}");
 }

 public String getLocalizedString(String language, String message) {
    String retVal = message;
    if( language.equals(RSBLocalizedException.JAPANESE) ) {
      String local = j.get(message);
      if(local != null ) retVal = local; 
    }
    return retVal;
  }
  
  public static String localizedMessage(String msg,
               //@  
                 Object... args
               //@
               /*#params Object[] args#*/
                ) {
     return RSBLocalizedException.localizedMessage(SqlExceptions.INSTANCE, msg, args);
  }
  
  public static RSBException Exception(String code, String msg,
               //@  
                 Object... args
               //@
               /*#params Object[] args#*/
                ) {
     return RSBLocalizedException.getException(SqlExceptions.INSTANCE, code, msg, args);
  }
} 
