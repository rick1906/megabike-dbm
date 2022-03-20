<?php

namespace megabike\db;

abstract class DbManager
{
    const TABLE_ENCLOSURE_BRACKETS = DbOperator::TABLE_ENCLOSURE_BRACKETS;
    const TABLE_ENCLOSURE_PREFIX = DbOperator::TABLE_ENCLOSURE_PREFIX;
    //
    const FETCH_ASSOC = DbOperator::FETCH_ASSOC;
    const FETCH_NUMERIC = DbOperator::FETCH_NUMERIC;
    const FETCH_BOTH = DbOperator::FETCH_BOTH;
    //
    const JOIN_INNER = DbOperator::JOIN_INNER;
    const JOIN_LEFT = DbOperator::JOIN_LEFT;
    const JOIN_CROSS = DbOperator::JOIN_CROSS;
    //
    const OPTIONS_NONE = DbOperator::OPTIONS_NONE;
    const OPTIONS_DISTINCT = DbOperator::OPTIONS_DISTINCT;
    const OPTIONS_IGNORE = DbOperator::OPTIONS_IGNORE;
    const OPTIONS_SINGLE_ROW = DbOperator::OPTIONS_SINGLE_ROW;

    public static $defaultDbParams = 'defaultDatabaseConfig';

    /**
     * @var DbOperator
     */
    protected static $operator = null;

    /**
     * @return DbOperator
     */
    public static function db()
    {
        return static::$operator !== null ? static::$operator : static::autoConnect();
    }

    /**
     * @return DbOperator
     */
    public static function getOperator()
    {
        return static::$operator;
    }

    public static function setOperator($operator)
    {
        static::$operator = $operator;
    }

    public static function autoConnect()
    {
        $params = static::$defaultDbParams;
        if ((!\is_array($params) || isset($params[0])) && \is_callable($params)) {
            $params = \call_user_func($params);
        }
        if ($params) {
            $db = DbOperator::create($params, true);
        } else {
            throw new \RuntimeException("Unable to autoconnect to database");
        }
        static::$operator = $db;
        return $db;
    }
    
    public static function close()
    {
        $result = static::disconnect();
        static::$operator = null;
        return $result;
    }

    public static function isConnected()
    {
        return static::$operator && static::$operator->isConnected();
    }

    public static function connect()
    {
        return static::db()->connect();
    }

    public static function disconnect()
    {
        return static::db()->disconnect();
    }

    public static function reconnect()
    {
        return static::$operator !== null ? static::$operator->reconnect() : (bool)static::db();
    }

    public static function getDatabaseType()
    {
        return static::db()->getDatabaseType();
    }

    public static function getLastErrorCode()
    {
        return static::db()->getLastErrorCode();
    }

    public static function getLastErrorMessage()
    {
        return static::db()->getLastErrorMessage();
    }

    public static function escapeRaw($string)
    {
        return static::db()->escapeRaw($string);
    }

    public static function escape($string)
    {
        return static::db()->escape($string);
    }

    public static function escapeMask($string)
    {
        return static::db()->escapeMask($string);
    }

    public static function setCharset($charset, $collation = null)
    {
        return static::db()->setCharset($charset, $collation);
    }

    public static function trySetCharset($charset, $collation = null)
    {
        return static::db()->trySetCharset($charset, $collation);
    }

    public static function selectDb($dbname)
    {
        return static::db()->selectDb($dbname);
    }

    public static function trySelectDb($dbname)
    {
        return static::db()->trySelectDb($dbname);
    }

    public static function execute($query, $raw = false)
    {
        return static::db()->execute($query, $raw);
    }

    public static function tryExecute($query, $raw = false)
    {
        return static::db()->tryExecute($query, $raw);
    }

    public static function query($query)
    {
        return static::db()->query($query);
    }

    public static function tryQuery($query)
    {
        return static::db()->tryQuery($query);
    }

    public static function queryRow($query, $fetchType = 0)
    {
        return static::db()->queryRow($query, $fetchType);
    }

    public static function queryScalar($query, $columnIndex = 0)
    {
        return static::db()->queryScalar($query, $columnIndex);
    }

    /**
     * @param string $query
     * @param int $fetchType
     * @return DbReader
     */
    public static function queryReader($query, $fetchType = 0)
    {
        return static::db()->queryReader($query, $fetchType);
    }

    public static function fetch($result, $fetchType = 0)
    {
        return static::db()->fetch($result, $fetchType);
    }

    public static function fetchAll($result, $fetchType = 0)
    {
        return static::db()->fetchAll($result, $fetchType);
    }

    public static function fetchAssoc($result)
    {
        return static::db()->fetchAssoc($result);
    }

    public static function fetchNumeric($result)
    {
        return static::db()->fetchNumeric($result);
    }

    public static function fetchBoth($result)
    {
        return static::db()->fetchBoth($result);
    }

    public static function fetchScalar($result, $columnIndex = 0)
    {
        return static::db()->fetchScalar($result, $columnIndex);
    }

    public static function fieldsCount($result)
    {
        return static::db()->fieldsCount($result);
    }

    public static function fieldsMetadata($result)
    {
        return static::db()->fieldsMetadata($result);
    }

    public static function fieldsNames($result)
    {
        return static::db()->fieldsNames($result);
    }

    public static function dataSeek($result, $offset)
    {
        return static::db()->dataSeek($result, $offset);
    }

    public static function numRows($result)
    {
        return static::db()->numRows($result);
    }

    public static function free($result)
    {
        return static::db()->free($result);
    }

    public static function affectedRows()
    {
        return static::db()->affectedRows();
    }

    public static function insertId()
    {
        return static::db()->insertId();
    }

    /**
     * 
     * @param mixed $result
     * @param int $fetchType
     * @return DbReader
     */
    public static function reader($result, $fetchType = 0)
    {
        return static::db()->reader($result, $fetchType);
    }

    public static function beginTransaction()
    {
        return static::db()->beginTransaction();
    }

    public static function commit()
    {
        return static::db()->commit();
    }

    public static function rollback()
    {
        return static::db()->rollback();
    }

    /**
     * 
     * @param string $query
     * @return DbStatement
     */
    public static function prepare($query)
    {
        return static::db()->prepare($query);
    }

    public static function resolveTableName($table)
    {
        return static::db()->resolveTableName($table);
    }

    public static function constant($value)
    {
        return static::db()->constant($value);
    }

    public static function constantArray($array)
    {
        return static::db()->constantArray($array);
    }

    public static function expression($string)
    {
        return static::db()->expression($string);
    }

    public static function param($name = null)
    {
        return static::db()->param($name);
    }

    public static function quoteString($string)
    {
        return static::db()->quoteString($string);
    }

    public static function quoteName($name)
    {
        return static::db()->quoteName($name);
    }

    public static function quoteNameOrExpression($name)
    {
        return static::db()->quoteNameOrExpression($name);
    }

    public static function quoteNameInSelect($name)
    {
        return static::db()->quoteNameInSelect($name);
    }

    public static function joinWhere($where1, $where2, $or = false)
    {
        return static::db()->joinWhere($where1, $where2, $or);
    }

    public static function mergeWhere($where1, $where2, $or = false)
    {
        return static::db()->mergeWhere($where1, $where2, $or);
    }

    public static function joinOrder($primaryOrder, $secondaryOrder)
    {
        return static::db()->joinOrder($primaryOrder, $secondaryOrder);
    }

    public static function mergeOrder($secondaryOrder, $primaryOrder)
    {
        return static::db()->mergeOrder($secondaryOrder, $primaryOrder);
    }

    public static function mergeFrom($from, $joins, $transform = false)
    {
        return static::db()->mergeFrom($from, $joins, $transform);
    }

    public static function mergeSelect($select1, $select2)
    {
        return static::db()->mergeSelect($select1, $select2);
    }

    public static function createSelect($select, $from = null, $where = null, $order = null, $limit = null, $groupBy = null, $having = null)
    {
        return static::db()->createSelect($select, $from, $where, $order, $limit, $groupBy, $having);
    }

    public static function createSelectDistinct($select, $from = null, $where = null, $order = null, $limit = null, $groupBy = null, $having = null)
    {
        return static::db()->createSelectDistinct($select, $from, $where, $order, $limit, $groupBy, $having);
    }

    public static function createInsert($table, $records)
    {
        return static::db()->createInsert($table, $records);
    }
    
    public static function createInsertUpdate($table, $insert, $update)
    {
        return static::db()->createInsertUpdate($table, $insert, $update);
    }

    public static function createInsertIgnore($table, $records)
    {
        return static::db()->createInsertIgnore($table, $records);
    }

    public static function createUpdate($table, $data, $where = null)
    {
        return static::db()->createUpdate($table, $data, $where);
    }

    public static function createUpdateIgnore($table, $data, $where = null)
    {
        return static::db()->createUpdateIgnore($table, $data, $where);
    }

    public static function createDelete($table, $where = null)
    {
        return static::db()->createDelete($table, $where);
    }

}
