<?php

namespace megabike\db;

abstract class DbOperator
{
    //TODO: query builder

    const TABLE_ENCLOSURE_BRACKETS = '{{';
    const TABLE_ENCLOSURE_PREFIX = '#pre#';
    //
    const FETCH_ASSOC = 0;
    const FETCH_NUMERIC = 1;
    const FETCH_BOTH = 2;
    //
    const JOIN_INNER = 'INNER JOIN';
    const JOIN_LEFT = 'LEFT JOIN';
    const JOIN_CROSS = 'CROSS JOIN';
    //
    const OPTIONS_NONE = 0x00;
    const OPTIONS_DISTINCT = 0x01;
    const OPTIONS_IGNORE = 0x0100;
    const OPTIONS_SINGLE_ROW = 0x400;

    private static $operatorsMap = array(
        'mysql' => 'mysqli',
    );

    public static function create($params, $connect = true)
    {
        if (empty($params['class'])) {
            if (empty($params['type'])) {
                throw new \InvalidArgumentException("No valid 'type' key in operator parameters");
            } else {
                $type = (string)$params['type'];
                $class = self::getOperatorClass($type);
            }
        } else {
            $class = (string)$params['class'];
        }
        self::loadClass($class);
        return new $class($params, $connect);
    }

    public static function getSpecificComponent($type, $name)
    {
        $class = __NAMESPACE__.'\\'.$type.'\\'.$name;
        return self::loadClass($class) ? $class : null;
    }

    public static function loadClass($class)
    {
        if (\class_exists($class, false)) {
            return true;
        } else {
            $ns = __NAMESPACE__.'\\';
            $ln = \strlen($ns);
            if (!\strncmp($class, $ns, $ln)) {
                $path = \dirname(__FILE__).DIRECTORY_SEPARATOR.\str_replace('\\', DIRECTORY_SEPARATOR, \substr($class, $ln)).'.php';
                if (\is_file($path)) {
                    require_once($path);
                    return \class_exists($class, false);
                } else {
                    return false;
                }
            }
            return false;
        }
    }

    private static function getOperatorClass($type)
    {
        if (isset(self::$operatorsMap[$type])) {
            $type = self::$operatorsMap[$type];
        }
        return __NAMESPACE__.'\\'.$type.\strrchr(__CLASS__, "\\");
    }

    protected $params = null;
    protected $connection = null;
    protected $autoClose = true;
    protected $tablePrefix = '';
    protected $tableEnclosure = self::TABLE_ENCLOSURE_PREFIX;
    protected $expressionChars = null;
    protected $expressionOrSpaceRegex = null;
    protected $enableEscapePrefix = true;
    protected $enableStrictMode = true;
    protected $enableForcePrepare = true;
    protected $forcePrepareCommands = array('select', 'call', 'show');
    protected $forcePrepareRegex = null;

    public function __construct($params, $connect = true)
    {
        $this->params = (array)$this->generateParams($params);
        $this->initialize($this->params);
        $this->initializeVariables();
        if ($this->isValidConnection($connect)) {
            $this->connection = $connect;
            $this->autoClose = false;
        } elseif ($connect) {
            $this->connect();
        }
    }

    public function __destruct()
    {
        if ($this->autoClose) {
            $this->disconnect();
        }
    }

    public function getOperatorType()
    {
        return !empty($this->params['type']) ? (string)$this->params['type'] : $this->getConnectionType();
    }

    public function getPreferredCharset()
    {
        return $this->getParam($this->params, 'charset', 'encoding');
    }

    public function getHost()
    {
        return $this->getParam($this->params, 'host', 'dbhost');
    }

    public function getPrimaryDatabase()
    {
        return $this->getParam($this->params, 'database', 'dbname');
    }

    protected function getUsername()
    {
        return $this->getParam($this->params, 'username', 'user', 'dbuser');
    }

    protected function getPassword()
    {
        return $this->getParam($this->params, 'password', 'passwd', 'dbpass');
    }

    public final function getTablePrefix()
    {
        return $this->tablePrefix;
    }

    public final function getConnection()
    {
        return $this->connection;
    }

    public final function isConnected()
    {
        return !empty($this->connection);
    }

    public final function isEscapePrefixEnabled()
    {
        return $this->enableEscapePrefix;
    }

    public function setEscapePrefixEnabled($value)
    {
        $this->enableEscapePrefix = (bool)$value;
    }

    public final function isStrictModeEnabled()
    {
        return $this->enableStrictMode;
    }

    public function setStrictModeEnabled($value)
    {
        $this->enableStrictMode = (bool)$value;
        if ($this->connection) {
            $this->initConnectionFlags();
        }
    }

    public final function getTableEnclosure()
    {
        return $this->tableEnclosure;
    }

    public function setForcePrepareEnabled($value)
    {
        $this->enableForcePrepare = (bool)$value;
    }

    public final function isForcePrepareEnabled()
    {
        return $this->enableForcePrepare;
    }

    public function setForcePrepareCommands($array)
    {
        $this->forcePrepareCommands = (array)$array;
        $this->forcePrepareRegex = '/^\W*('.\implode('|', $this->forcePrepareCommands).')\W/i';
    }

    public final function getForcePrepareCommands()
    {
        return $this->forcePrepareCommands;
    }

    public function setTableEnclosure($value)
    {
        if ($value === self::TABLE_ENCLOSURE_BRACKETS || $value === self::TABLE_ENCLOSURE_PREFIX) {
            $this->tableEnclosure = $value;
        } else {
            throw new \InvalidArgumentException('Invalid table enclosure type');
        }
    }

    protected final function getParam($params, $name)
    {
        if (isset($params[$name])) {
            return $params[$name];
        } elseif (\func_num_args() > 2) {
            $names = \array_slice(\func_get_args(), 2);
            foreach ($names as $n) {
                if (isset($params[$n])) {
                    return $params[$n];
                }
            }
        }
        return null;
    }

    protected function initialize($params)
    {
        if (($tablePrefix = $this->getParam($params, 'tablePrefix', 'prefix')) !== null) {
            $this->tablePrefix = (string)$tablePrefix;
        }
        if (($enableEscapePrefix = $this->getParam($params, 'escapeTablePrefix', 'escapePrefix')) !== null) {
            $this->enableEscapePrefix = (bool)$enableEscapePrefix;
        }
        if (($enableStrictMode = $this->getParam($params, 'strictMode', 'strict')) !== null) {
            $this->enableStrictMode = (bool)$enableStrictMode;
        }
        if (($enableForcePrepare = $this->getParam($params, 'forcePrepare')) !== null) {
            $this->enableForcePrepare = (bool)$enableForcePrepare;
        }
        if (($forcePrepareCommands = $this->getParam($params, 'forcePrepareCommands', 'prepareCommands')) !== null) {
            $this->forcePrepareCommands = (array)$forcePrepareCommands;
        }
    }

    protected function initializeVariables()
    {
        $this->setForcePrepareCommands($this->forcePrepareCommands);
        if ($this->expressionChars === null) {
            $this->expressionChars = '{('.\implode('', \array_unique(\str_split($this->quoteSimpleName(''))));
        }
        if ($this->expressionOrSpaceRegex === null) {
            $this->expressionOrSpaceRegex = '/[\s'.\preg_quote($this->expressionChars, '/').']/';
        }
    }

    protected function generateParams($params)
    {
        if (\is_array($params) || $params === null) {
            return (array)$params;
        } else {
            throw new \InvalidArgumentException("Invalid connection parameters supplied");
        }
    }

    public function connect($reuseOpened = true)
    {
        if ($this->connection) {
            if ($reuseOpened) {
                return $this->connection;
            } else {
                throw new DbException("Connection is already open");
            }
        }
        $connection = $this->openConnection();
        if ($connection) {
            $this->connection = $connection;
            $this->initConnectionFlags();
            $this->initConnectionCharset();
            $this->onAfterConnected();
            return $connection;
        } else {
            throw new DbException("Unable to create a connection with supplied parameters");
        }
    }

    public function disconnect()
    {
        if ($this->connection) {
            $result = $this->closeConnection();
            $this->connection = null;
            return $result;
        }
        return false;
    }

    public function reconnect()
    {
        $this->disconnect();
        return $this->connect(false);
    }

    public function copy($connect = false)
    {
        $class = \get_class($this);
        return new $class($this->params, $connect);
    }

    public abstract function getDatabaseType();

    protected abstract function getConnectionType();

    protected abstract function openConnection();

    protected abstract function closeConnection();

    protected abstract function isValidConnection($connection);

    protected function initConnectionFlags()
    {
        
    }

    protected function initConnectionCharset()
    {
        $charset = $this->getPreferredCharset();
        if ((string)$charset !== '' && !$this->setCharset($charset)) {
            throw new DbException("Unable to set charset '{$charset}'");
        }
    }

    protected function onAfterConnected()
    {
        
    }

    public abstract function getLastErrorCode();

    public abstract function getLastErrorMessage();

    public abstract function escapeRaw($string);

    public function escape($string)
    {
        if ($this->enableEscapePrefix) {
            return $this->escapeRaw($this->escapeTablePrefix($string));
        } else {
            return $this->escapeRaw($string);
        }
    }

    public function escapeMask($string)
    {
        return \addcslashes($this->escape($string), '%_');
    }

    protected function escapeTablePrefix($string)
    {
        if ($this->tableEnclosure === self::TABLE_ENCLOSURE_BRACKETS) {
            return \str_replace('{{', '@{{', $string);
        } elseif ($this->tableEnclosure === self::TABLE_ENCLOSURE_PREFIX) {
            return \str_replace('#pre#', '@#pre#', $string);
        } else {
            return $string;
        }
    }

    protected function applyTablePrefix($string, $unescape = true)
    {
        if ($this->tableEnclosure === self::TABLE_ENCLOSURE_BRACKETS) {
            $search = '/(?<!@){{(.*?)}}/';
            $replace = $this->tablePrefix.'\1';
            $escaped = '/@(?={{)/';
        } elseif ($this->tableEnclosure === self::TABLE_ENCLOSURE_PREFIX) {
            $search = '/(?<!@)#pre#/';
            $replace = $this->tablePrefix;
            $escaped = '/@(?=#pre#)/';
        } else {
            return $string;
        }
        if ($unescape) {
            return \preg_replace(array($search, $escaped), array($replace, ''), $string);
        } else {
            return \preg_replace($search, $replace, $string);
        }
    }

    protected function prepareQueryString($query)
    {
        return $this->applyTablePrefix($query, $this->enableEscapePrefix);
    }

    protected abstract function executeSetCharset($charset, $collation = null);

    protected abstract function executeSelectDb($dbname);

    protected abstract function executeSimpleQuery($query);

    protected abstract function executeQuery($query);

    public final function setCharset($charset, $collation = null)
    {
        $exception = null;
        try {
            $result = $this->executeSetCharset($charset, $collation);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Charset selection"), $exception);
        }
        return $result;
    }

    public final function trySetCharset($charset, $collation = null)
    {
        $exception = null;
        try {
            $result = $this->executeSetCharset($charset, $collation);
        } catch (\Exception $exception) {
            $result = false;
        }
        return $result;
    }

    public final function selectDb($dbname)
    {
        $exception = null;
        try {
            $result = $this->executeSelectDb($dbname);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Selecting database"), $exception);
        }
        return $result;
    }

    public final function trySelectDb($dbname)
    {
        $exception = null;
        try {
            $result = $this->executeSelectDb($dbname);
        } catch (\Exception $exception) {
            $result = false;
        }
        return $result;
    }

    public final function execute($query, $raw = false)
    {
        $exception = null;
        $internalQuery = $raw ? $query : $this->prepareQueryString($query);
        try {
            $result = $this->executeSimpleQuery($internalQuery);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Query", $internalQuery), $exception);
        }
        return $result;
    }

    public final function tryExecute($query, $raw = false)
    {
        $exception = null;
        $internalQuery = $raw ? $query : $this->prepareQueryString($query);
        try {
            $result = $this->executeSimpleQuery($internalQuery);
        } catch (\Exception $exception) {
            $result = false;
        }
        return $result;
    }

    public final function query($query)
    {
        $exception = null;
        $internalQuery = $this->prepareQueryString($query);
        try {
            $result = $this->executeQuery($internalQuery);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Query", $internalQuery), $exception);
        }
        return $result;
    }

    public final function tryQuery($query)
    {
        $exception = null;
        $internalQuery = $this->prepareQueryString($query);
        try {
            $result = $this->executeQuery($internalQuery);
        } catch (\Exception $exception) {
            $result = false;
        }
        return $result;
    }

    public function queryRow($query, $fetchType = 0)
    {
        $result = $this->query($query);
        return $this->fetch($result, $fetchType);
    }

    public function queryScalar($query, $columnIndex = 0)
    {
        $result = $this->query($query);
        return $this->fetchScalar($result, $columnIndex);
    }

    /**
     * @param string $query
     * @param int $fetchType
     * @return DbReader
     */
    public function queryReader($query, $fetchType = 0)
    {
        $result = $this->query($query);
        return $this->reader($result, $fetchType);
    }

    public function fetch($result, $fetchType = 0)
    {
        if ($fetchType === self::FETCH_ASSOC) {
            return $this->fetchAssoc($result);
        } elseif ($fetchType === self::FETCH_NUMERIC) {
            return $this->fetchNumeric($result);
        } elseif ($fetchType === self::FETCH_BOTH) {
            return $this->fetchBoth($result);
        } else {
            throw new DbException("Unsupported fetch type");
        }
    }

    public function fetchAll($result, $fetchType = 0)
    {
        $rows = array();
        while ($row = $this->fetch($result, $fetchType)) {
            $rows[] = $row;
        }
        return $rows;
    }

    public abstract function fetchAssoc($result);

    public abstract function fetchNumeric($result);

    public abstract function fetchBoth($result);

    public abstract function fetchScalar($result, $columnIndex = 0);

    public abstract function fieldsCount($result);

    public abstract function fieldsMetadata($result);

    public abstract function fieldsNames($result);

    public abstract function dataSeek($result, $offset);

    public abstract function numRows($result);

    public abstract function free($result);

    public abstract function affectedRows();

    public abstract function insertId();

    /**
     * 
     * @param mixed $result
     * @param int $fetchType
     * @return DbReader
     */
    public function reader($result, $fetchType = 0)
    {
        return new DbReader($this, $result, $fetchType);
    }

    public abstract function supportsTransactions();

    public abstract function beginTransaction();

    public abstract function commit();

    public abstract function rollback();

    public abstract function supportsPreparedStatements();

    protected function getStatementComponent()
    {
        $type = $this->getConnectionType();
        return self::getSpecificComponent($type, 'DbStatement');
    }

    /**
     * 
     * @param string $query
     * @return DbStatement
     */
    public function prepare($query)
    {
        if ($this->supportsPreparedStatements()) {
            $class = $this->getStatementComponent();
            if ($class !== null) {
                return new $class($this, $this->prepareQueryString($query));
            }
        }
        throw new DbException("Statements are not supported");
    }

    public function resolveTableName($table)
    {
        return $this->applyTablePrefix($table, false);
    }

    public function constant($value)
    {
        if (\is_string($value)) {
            return $this->quoteString($value);
        } elseif (\is_int($value) || \is_float($value)) {
            return $value;
        } elseif (\is_bool($value)) {
            return $value ? 1 : 0;
        } elseif ($value === null) {
            return 'NULL';
        } elseif ($value instanceof \DateTime) {
            return $value->format('Y-m-d H:i:s');
        } elseif ($value instanceof DbExpression) {
            return (string)$value;
        } else {
            return $this->quoteString((string)$value);
        }
    }

    public function constantArray($array)
    {
        $parts = array();
        foreach ((array)$array as $value) {
            $parts[] = $this->constant($value);
        }
        return \implode(',', $parts);
    }

    public function expression($string)
    {
        return new DbExpression($string);
    }

    public function param($name = null)
    {
        return new DbParameter($name);
    }

    public function quoteString($string)
    {
        return $this->quoteEscapedString($this->escape($string));
    }

    protected abstract function quoteEscapedString($string);

    protected abstract function quoteSimpleName($name);

    public function quoteName($name) // NOTE: table aliases are never quoted (alias.`field`)
    {
        $parts = explode('.', $name, 2);
        if (isset($parts[1])) {
            $t = \rtrim($parts[0]);
            $f = \ltrim($parts[1]);
            if ($f !== '*') {
                $f = $this->quoteSimpleName($f);
            }
            return $t.'.'.$f;
        }
        return $this->quoteSimpleName($name);
    }

    public function quoteNameOrExpression($name)
    {
        if (\strpbrk($name, $this->expressionChars) !== false) {
            return $name;
        } else {
            return $this->quoteName($name);
        }
    }

    public function quoteNameInSelect($name)
    {
        if (\preg_match($this->expressionOrSpaceRegex, $name)) {
            return $name;
        } else {
            return $this->quoteName($name);
        }
    }

    protected function booleanCondition($boolean)
    {
        return $boolean ? 'TRUE' : 'FALSE';
    }

    protected function buildLikeConstraint($likeOp, $value)
    {
        $pos = \strpos($likeOp, '%');
        $len = \strlen($likeOp);
        $op = \str_replace('%', '', $likeOp);
        if ($value instanceof DbExpression) {
            if ($pos !== false) {
                throw new DbException("Expressions are not supported in %LIKE% operators");
            } else {
                return array($op, $value->getExpression());
            }
        } elseif ($pos !== false) {
            $right = $pos + 1 >= $len;
            if ($right) { // only right %
                return array($op, $this->quoteEscapedString($this->escapeMask((string)$value).'%'));
            } else {
                $right = \substr($likeOp, -1) === '%';
                if ($right) { // both %
                    return array($op, $this->quoteEscapedString('%'.$this->escapeMask((string)$value).'%'));
                } else { // only left %
                    return array($op, $this->quoteEscapedString('%'.$this->escapeMask((string)$value)));
                }
            }
        } else {
            return array($op, $this->quoteEscapedString($this->escapeMask((string)$value)));
        }
    }

    protected function assembleCondition($field, $upperCaseOp, $expression)
    {
        if ($upperCaseOp === 'IN' || $upperCaseOp === 'NOT IN') {
            if ($expression === '') {
                $this->booleanCondition($upperCaseOp !== 'IN');
            }
            return $this->quoteNameOrExpression($field)." {$upperCaseOp} ({$expression})";
        }
        return $this->quoteNameOrExpression($field)." {$upperCaseOp} {$expression}";
    }

    protected function transformOperator($upperCaseOp, $value)
    {
        if ($value === null) {
            if ($upperCaseOp === '=') {
                return 'IS';
            } elseif ($upperCaseOp === '<>' || $upperCaseOp === '!=') {
                return 'IS NOT';
            }
        }
        return $upperCaseOp;
    }

    protected function buildScalarCondition($field, $value)
    {
        return $this->assembleCondition($field, $this->transformOperator('=', $value), $this->constant($value));
    }

    protected function buildCondition($field, $operator, $value)
    {
        if ((string)$operator !== '') {
            $operator = $this->transformOperator(\strtoupper($operator), $value);
            if (\strpos($operator, 'LIKE') !== false) {
                list($operator, $expression) = $this->buildLikeConstraint($operator, $value);
                return $this->assembleCondition($field, $operator, $expression);
            }
        } else {
            $operator = $this->transformOperator('=', $value);
        }
        if ($value instanceof DbExpression) {
            $expression = $value->getExpression();
        } elseif (\is_array($value)) {
            $expression = $this->constantArray($value);
        } else {
            $expression = $this->constant($value);
        }
        return $this->assembleCondition($field, $operator, $expression);
    }

    protected function transformConditionValue($value, $parameters = null)
    {
        if ($value instanceof DbExpression) {
            return $value->buildValue($this, $parameters);
        }
        return $value;
    }

    protected function buildFieldConditions($field, $condition, $parameters = null)
    {
        if (!\is_array($condition)) {
            $value = $this->transformConditionValue($condition, $parameters);
            return $this->buildCondition($field, '', $value);
        }
        if (\array_key_exists(0, $condition) && !\is_array($condition[0]) && \count($condition) <= 2) {
            $item = \array_values($condition);
            $op = (string)$condition[0];
            if (\array_key_exists(1, $condition)) {
                $value = $this->transformConditionValue($condition[1], $parameters);
                return $this->buildCondition($field, $op, $value);
            } elseif (\count($condition) === 1) {
                return $this->buildCondition($field, $op, null);
            }
        }

        $results = array();
        foreach ($condition as $key => $item) {
            if (\is_int($key)) {
                $result = $this->buildFieldConditions($field, $item, $parameters);
            } else {
                $value = $this->transformConditionValue($item, $parameters);
                $result = $this->buildCondition($field, $key, $value);
            }
            if ($result) {
                $results[] = $result;
            }
        }
        return $this->joinConditions('AND', $results);
    }

    protected function joinConditions($upperCaseOp, $conditions)
    {
        if (empty($conditions)) {
            return '';
        }
        if ($upperCaseOp === 'OR' && \count($conditions) > 1) {
            return '('.\implode(" {$upperCaseOp} ", $conditions).')';
        }
        return \implode(" {$upperCaseOp} ", $conditions);
    }

    public function buildWhereString($where, $parameters = null)
    {
        if (\is_array($where)) {
            return $this->buildWhereFromArray($where, $parameters);
        } elseif (\is_bool($where)) { // boolean
            return '';
        } elseif ($where instanceof DbExpression) { // expression
            return (string)$where->buildExpression($this, $parameters);
        } else {
            return (string)$where;
        }
    }

    protected function buildWhereFromArray($where, $parameters)
    {
        $op = 'AND';
        $wkeys = \array_keys($where);
        $wparts = array();
        if (isset($wkeys[0]) && $wkeys[0] === 0 && \is_string($where[0])) {
            $wfirst = (string)$where[0];
            if (!\strcasecmp($wfirst, 'AND') || !\strcasecmp($wfirst, 'OR')) {
                $op = \strtoupper($wfirst);
                $where = \array_slice($where, 1);
            } elseif (!\strcasecmp($wfirst, 'NOT')) {
                $expr = $this->buildWhereFromArray(\array_slice($where, 1), $parameters);
                return (string)$expr !== '' ? '\NOT('.$expr.')' : '';
            }
        }
        foreach ($where as $key => $val) {
            if (\is_int($key)) {
                if (\is_string($val) || $val === null) {
                    $expr = \trim((string)$val);
                } else {
                    $expr = $this->buildWhereString($val, $parameters);
                }
            } else {
                if (\is_scalar($val) || $val === null) {
                    $expr = $this->buildScalarCondition($key, $val);
                } else {
                    $expr = $this->buildFieldConditions($key, $val, $parameters);
                }
            }
            if ((string)$expr !== '') {
                $wparts[] = $expr;
            }
        }
        return $this->joinConditions($op, $wparts);
    }

    protected function isEmptyExpression($expression)
    {
        return empty($expression) && $expression !== '0' && $expression !== 0 || \is_bool($expression);
    }

    public function joinWhere($where1, $where2, $or = false)
    {
        if ($this->isEmptyExpression($where1)) {
            return $where2;
        }
        if ($this->isEmptyExpression($where2)) {
            return $where1;
        }
        $op = $or ? 'OR' : 'AND';
        return array($op, $where1, $where2);
    }

    public function mergeWhere($where1, $where2, $or = false)
    {
        if (\is_string($where1) && \is_string($where2) && $where1 !== '' && $where2 !== '') {
            return $this->joinConditions($or ? 'OR' : 'AND', array($where1, $where2));
        }
        if ($or) {
            return $this->joinWhere($where1, $where2, true);
        }
        if (\is_array($where1) && \is_array($where2) && !empty($where1) && !empty($where2)) {
            $wkeys1 = \array_keys($where1);
            $wkeys2 = \array_keys($where2);
            $notop1 = $wkeys1[0] !== 0 || \is_array($where1[$wkeys1[0]]) || !\preg_match('/^\w+$/', $where1[$wkeys1[0]]);
            $notop2 = $wkeys2[0] !== 0 || \is_array($where2[$wkeys2[0]]) || !\preg_match('/^\w+$/', $where2[$wkeys2[0]]);
            if ($notop1 && $notop2) {
                $where = $where1;
                foreach ($where2 as $key => $value) {
                    if (\is_int($key)) {
                        $where[] = $value;
                    } elseif (!isset($where[$key])) {
                        $where[$key] = $value;
                    } else {
                        $extra = $where[$key];
                        $cond1 = \is_array($value) ? $value : array(null, $value);
                        $cond2 = \is_array($extra) ? $extra : array(null, $extra);
                        $where[$key] = array($cond1, $cond2);
                    }
                }
                return $where;
            }
        }
        return $this->joinWhere($where1, $where2, false);
    }

    public function buildOrderString($order, $parameters = null)
    {
        if (\is_array($order)) {
            return $this->buildOrderFromArray($order, $parameters);
        } elseif (\is_bool($order)) { // boolean
            return '';
        } elseif ($order instanceof DbExpression) {
            return (string)$order->buildExpression($this, $parameters);
        } else {
            return (string)$order;
        }
    }

    protected function buildOrderFromArray($order, $parameters = null)
    {
        $item = $this->extractOrderItem($order);
        if ($item !== false) {
            return $this->buildOrderItem($item[0], $item[1], $parameters);
        }

        $oparts = array();
        foreach ($order as $key => $val) {
            if (\is_int($key)) {
                if (!\is_array($val)) {
                    $expr = $this->buildOrderItem($val, false, $parameters);
                } else {
                    $expr = $this->buildOrderFromArray($val, $parameters);
                }
            } else {
                $expr = $this->buildOrderItem($key, $val, $parameters);
            }
            if ((string)$expr !== '') {
                $oparts[] = $expr;
            }
        }
        return $this->joinOrderItems($oparts);
    }

    protected function joinOrderItems($items)
    {
        return \implode(', ', $items);
    }

    protected function extractOrderItem($order)
    {
        if (\is_array($order) && \array_key_exists(0, $order) && \array_key_exists(1, $order) && !\is_array($order[1]) && \count($order) === 2) {
            $op = $order[1];
            if ($op === null || \is_scalar($op)) {
                $op = $this->extractOrderOperator($op, true);
                if ($op !== false) {
                    return array($order[0], $op);
                }
            }
        }
        return false;
    }

    protected function extractOrderOperator($string, $strict = false)
    {
        if ((string)$string === '') {
            return '';
        } else {
            $op = \strtoupper($string);
            if ($strict) {
                return ($op === 'ASC' || $op === 'DESC') ? $op : false;
            } else {
                $len = \strlen($op);
                if (!\strncmp('ASCENDING', $op, $len)) {
                    return 'ASC';
                }
                if (!\strncmp('DESCENDING', $op, $len)) {
                    return 'DESC';
                }
                return false;
            }
        }
    }

    protected function buildOrderItem($field, $order, $parameters = null)
    {
        if ($order === false && \is_string($field) && \preg_match('/\s/', $field)) { // order type in field
            return $field;
        } else {
            $op = $this->extractOrderOperator((string)$order);
        }
        if ($op === false) { // invalid order type
            throw new DbException("Invalid order type '{$order}'");
        }
        if ($field instanceof DbExpression) {
            $expr = '('.(string)$field->buildExpression($this, $parameters).')';
        } else {
            $expr = $this->quoteNameOrExpression($field);
        }
        return $op !== '' ? ($expr.' '.$op) : $expr;
    }

    public function joinOrder($primaryOrder, $secondaryOrder)
    {
        if ($this->isEmptyExpression($primaryOrder)) {
            return $secondaryOrder;
        }
        if ($this->isEmptyExpression($secondaryOrder)) {
            return $primaryOrder;
        }
        if (\is_string($primaryOrder) && \is_string($secondaryOrder)) {
            return $this->joinOrderItems(array($primaryOrder, $secondaryOrder));
        }
        if (\is_array($primaryOrder) && \is_array($secondaryOrder)) {
            $okeys1 = \array_keys($primaryOrder);
            $okeys2 = \array_keys($secondaryOrder);
            $item1 = $this->extractOrderItem($primaryOrder);
            $item2 = $this->extractOrderItem($secondaryOrder);
            if ($item1 === null && $item2 === null && \array_keys($okeys1) === $okeys1 && \array_keys($okeys2) === $okeys2) {
                return \array_merge($primaryOrder, $secondaryOrder);
            }
        }
        return array($primaryOrder, $secondaryOrder);
    }

    public function mergeOrder($secondaryOrder, $primaryOrder)
    {
        if ($this->isEmptyExpression($primaryOrder)) {
            return $secondaryOrder;
        }
        if ($this->isEmptyExpression($secondaryOrder)) {
            return $primaryOrder;
        }
        if (\is_string($primaryOrder) && \is_string($secondaryOrder)) {
            return $this->joinOrderItems(array($primaryOrder, $secondaryOrder));
        }
        if (\is_array($primaryOrder) && \is_array($secondaryOrder)) {
            $item1 = $this->extractOrderItem($primaryOrder);
            $item2 = $this->extractOrderItem($secondaryOrder);
            if ($item1 === null && $item2 === null) {
                $order = \array_merge($primaryOrder, $secondaryOrder);
                foreach ($primaryOrder as $key => $value) {
                    if (\is_string($key)) {
                        $order[$key] = $value;
                    }
                }
                return $order;
            }
        }
        return array($primaryOrder, $secondaryOrder);
    }

    public function buildFromString($from, $parameters = null)
    {
        $table = null;
        $joins = null;
        if (!\is_array($from)) { // expression
            $table = $this->buildNameItem($from, $parameters);
        } else {
            $fkeys = \array_keys($from);
            if (isset($fkeys[0]) && $fkeys[0] === 0) {
                $table = $this->buildNameItem($from[0], $parameters);
                $joins = \array_slice($from, 1);
            }
        }
        if ((string)$table === '') {
            throw new DbException("Invalid main table name in FROM clause");
        }
        if ($joins) {
            $jparts = array();
            foreach ($joins as $key => $val) {
                if (\is_int($key)) {
                    $expr = $this->buildJoinClause($val, $parameters);
                } else {
                    $expr = $this->buildJoinClause($this->transformJoin($key, $val), $parameters);
                }
                if ((string)$expr !== '') {
                    $jparts[] = $expr;
                }
            }
            return $this->combineFrom($table, $jparts);
        }
        return $this->combineFrom($table);
    }

    protected function combineFrom($table, $joins = null)
    {
        if ($joins) {
            return $table."\n".\implode("\n", $joins);
        }
        return $table;
    }

    public function mergeFrom($from, $joins, $transform = false)
    {
        $from = \is_array($from) ? $from : array($from);
        if ($transform) {
            if (\is_array($joins) && isset($joins[0])) {
                $joins[0] = array('', $joins[0]);
            } else {
                $joins = array('', $joins);
            }
        }
        if (!\is_array($joins)) {
            $from[] = (string)$joins;
        } else {
            foreach ($joins as $join) {
                $from[] = $join;
            }
        }
        return $from;
    }

    public function buildJoinClause($join, $parameters = null)
    {
        if (!\is_array($join)) {
            return (string)$join;
        }
        if (empty($join)) {
            return '';
        }

        $ix = 0;
        $jkeys = \array_keys($join);
        if (isset($jkeys[$ix]) && $jkeys[$ix] === $ix) {
            $op = $this->transformJoinOperator($join[$ix]);
        } else {
            throw new DbException("No operator in JOIN clause");
        }
        if ($op !== false) {
            $ix++;
        }
        if (isset($jkeys[$ix]) && $jkeys[$ix] === $ix) {
            $table = $this->buildNameItem($join[$ix], $parameters);
        } else {
            throw new DbException("No table in JOIN clause");
        }
        if ($table === false && $op === false) {
            throw new DbException("Invalid operator in JOIN clause");
        } elseif ($table === false) {
            throw new DbException("Invalid table name in JOIN clause");
        } else {
            $ix++;
        }
        if (isset($jkeys[$ix]) && $jkeys[$ix] === $ix && \count($join) === $ix + 1) {
            $on = $this->buildWhereString($join[$ix], $parameters);
        } else {
            $on = $this->buildWhereString(\array_slice($join, $ix), $parameters);
        }
        if ((string)$op === '') {
            $op = $this->transformJoinOperator('');
        }
        if ((string)$on !== '') {
            return $op.' '.$table.' ON ('.$on.')';
        } else {
            return $op.' '.$table;
        }
    }

    protected function transformJoin($table, $on)
    {
        $mark = \substr($table, 0, 1);
        if ($mark === '@') {
            $table = \substr($table, 1);
            $op = 'LEFT JOIN';
        } else {
            $op = 'INNER JOIN';
        }
        return array($op, $table, $on);
    }

    protected function transformJoinOperator($operator)
    {
        if ((string)$operator === '') {
            return 'INNER JOIN';
        } else {
            $op = \strtoupper($operator);
            if ($op === 'INNER' || $op === 'INNER JOIN') {
                return 'INNER JOIN';
            }
            if ($op === 'LEFT' || $op === 'LEFT JOIN') {
                return 'LEFT JOIN';
            }
            if ($op === 'CROSS' || $op === 'CROSS JOIN') {
                return 'CROSS JOIN';
            }
            return false;
        }
    }

    protected function buildNameItem($item, $parameters = null)
    {
        if (!\is_array($item)) {
            $name = $item;
            $as = null;
        } elseif (\array_key_exists(0, $item) && \array_key_exists(1, $item) && \count($item) === 2) {
            $name = $item[0];
            $as = $item[1];
            if (\is_array($name) || \is_array($as)) {
                return false;
            }
        } else {
            return false;
        }
        if ($name instanceof DbExpression) {
            $name = '('.(string)$name->buildExpression($this, $parameters).')';
        } elseif ($as !== null) {
            $name = $this->quoteNameOrExpression((string)$name);
        } else {
            $name = $this->quoteNameInSelect((string)$name);
        }
        if ($as !== null) {
            return $this->buildNameWithAlias($name, $as);
        } else {
            return $name;
        }
    }

    protected function buildNameWithAlias($nameExpression, $alias)
    {
        return (string)$alias !== '' ? ($nameExpression.' '.$alias) : $nameExpression;
    }

    public function buildLimitArray($limit, $parameters = null)
    {
        if (empty($limit)) {
            return null;
        }
        if (\is_string($limit) && strpos($limit, ',') !== false) {
            $limit = explode(',', $limit, 2);
        }
        if (!\is_array($limit)) {
            $offset = 0;
        } else {
            if (\array_key_exists(0, $limit)) {
                if (\array_key_exists(1, $limit) && \count($limit) === 2) {
                    $offset = $limit[0];
                    $limit = $limit[1];
                } elseif (\count($limit) === 1) {
                    $offset = 0;
                    $limit = $limit[0];
                } else {
                    throw new DbException("Too many values in LIMIT array");
                }
            } else {
                throw new DbException("Invalid values in LIMIT array");
            }
        }
        if (!\is_int($limit)) {
            if ($limit === null || \is_numeric($limit)) {
                $limit = (int)$limit;
            } elseif ($limit instanceof DbExpression) {
                $limit = (string)$limit->buildExpression($this, $parameters);
            } else {
                throw new DbException("Invalid limit value in LIMIT clause");
            }
        }
        if (!\is_int($offset)) {
            if ($offset === null || \is_numeric($offset)) {
                $offset = (int)$offset;
            } elseif ($offset instanceof DbExpression) {
                $offset = (string)$offset->buildExpression($this, $parameters);
            } else {
                throw new DbException("Invalid offset value in LIMIT clause");
            }
        }
        return array($offset, $limit);
    }

    public function buildSelectString($select, $parameters = null)
    {
        if (\is_bool($select)) {
            return '*';
        }
        if (!\is_array($select)) {
            $select = (string)$select;
            return $select === '' ? '*' : $select;
        }

        $sparts = array();
        foreach ($select as $value) {
            $name = $this->buildNameItem($value, $parameters);
            if ($name === false) {
                throw new DbException("Invalid item name in SELECT clause");
            }
            if ((string)$name !== '') {
                $sparts[] = (string)$name;
            }
        }
        if (empty($sparts)) {
            return '*';
        }
        return $this->joinSelectItems($sparts);
    }

    protected function joinSelectItems($items)
    {
        return \implode(', ', $items);
    }

    public function mergeSelect($select1, $select2)
    {
        if (\is_array($select1) && \is_array($select2)) {
            return \array_merge($select1, $select2);
        }
        if (\is_string($select1) && \is_string($select2) && $select1 !== '' && $select2 !== '') {
            return $this->joinSelectItems(array($select1, $select2));
        }
        if ($this->isEmptyExpression($select1)) {
            return $select2;
        }
        if ($this->isEmptyExpression($select2)) {
            return $select1;
        }
        if (!\is_array($select1)) {
            $select1 = array((string)$select1);
        }
        if (!\is_array($select2)) {
            $select2 = array((string)$select2);
        }
        return \array_merge($select1, $select2);
    }

    public function buildSelectQuery($select, $from = null, $where = null, $order = null, $limit = null, $groupBy = null, $having = null, $options = null, $parameters = null, $command = null)
    {
        $command = $command !== null ? (string)$command : 'SELECT';
        $select = $this->buildSelectString($select, $parameters);
        $from = $from !== null ? $this->buildFromString($from, $parameters) : '';
        $where = $where !== null ? $this->buildWhereString($where, $parameters) : '';
        $order = $order !== null ? $this->buildOrderString($order, $parameters) : '';
        $limit = $limit !== null ? $this->buildLimitArray($limit, $parameters) : null;
        $groupBy = $groupBy !== null ? $this->buildOrderString($groupBy, $parameters) : '';
        $having = $having !== null ? $this->buildWhereString($having, $parameters) : '';
        return $this->combineSelectQuery($command, $select, $from, $where, $order, $limit, $groupBy, $having, $options);
    }

    protected function combineSelectQuery($command, $select, $from, $where, $order, $limit, $groupBy, $having, $options = null)
    {
        $body = $select;
        if ((string)$from !== '') {
            $body .= "\nFROM ".$from;
        }
        if ((string)$where !== '') {
            $body .= "\nWHERE ".$where;
        }
        if ((string)$groupBy !== '') {
            $body .= "\nGROUP BY ".$groupBy;
        }
        if ((string)$having !== '') {
            $body .= "\nHAVING ".$having;
        }
        if ((string)$order !== '') {
            $body .= "\nORDER BY ".$order;
        }
        if ($options) {
            if ($options & self::OPTIONS_SINGLE_ROW) {
                $limit = array($limit ? $limit[0] : 0, 1);
            }
            if ($options & self::OPTIONS_DISTINCT) {
                $command = $command.' DISTINCT';
            }
        }
        if ($limit) {
            return "{$command} {$body}\nLIMIT ".\implode(', ', $limit);
        } else {
            return "{$command} {$body}";
        }
    }

    public function buildInsertQuery($table, $records, $options = null, $parameters = null, $command = null)
    {
        $command = $command !== null ? (string)$command : 'INSERT';
        $body = $this->generateInsertBody($table, $records, $parameters);
        if ($body === false) {
            return false;
        }
        if ($options) {
            if ($options & self::OPTIONS_IGNORE) {
                $command .= ' IGNORE';
            }
        }
        return "{$command} {$body}";
    }

    public function buildInsertUpdateQuery($table, $insert, $update, $options = null, $parameters = null, $command = null)
    {
        if ($update) {
            $baseQuery = $this->buildInsertQuery($table, $insert, $options, $parameters, $command);
            $assignmentList = $this->generateAssignmentList($update, $parameters);
            return $baseQuery." ON DUPLICATE KEY UPDATE {$assignmentList}";
        } else {
            $options = $options ? ($options | self::OPTIONS_IGNORE) : self::OPTIONS_IGNORE;
            return $this->buildInsertQuery($table, $insert, $options, $parameters, $command);
        }
    }

    protected function generateInsertBody($table, $records, $parameters = null)
    {
        if (!\is_array($records) || empty($records)) {
            return false;
        }

        $keys = \array_keys($records);
        $multiple = $keys === \array_keys($keys);
        $fparts = array();
        if ($multiple) {
            $vmparts = array();
            foreach (\array_keys($records[0]) as $field) {
                $fparts[] = $this->quoteSimpleName($field);
            }
            foreach ($records as $record) {
                $vparts = array();
                foreach ($record as $value) {
                    $vparts[] = ($value instanceof DbExpression ? $value->buildExpression($this, $parameters) : $this->constant($value));
                }
                $vmparts[] = '('.\implode(', ', $vparts).')';
            }
            $body = '('.\implode(', ', $fparts).") VALUES\n".\implode("\n,", $vmparts);
        } else {
            $vparts = array();
            foreach ($records as $field => $value) {
                $fparts[] = $this->quoteSimpleName($field);
                $vparts[] = ($value instanceof DbExpression ? $value->buildExpression($this, $parameters) : $this->constant($value));
            }
            $body = '('.\implode(', ', $fparts).') VALUES ('.\implode(', ', $vparts).')';
        }
        return 'INTO '.$this->quoteNameOrExpression($table).' '.$body;
    }

    public function buildUpdateQuery($table, $data, $where = null, $options = null, $parameters = null, $command = null)
    {
        $command = $command !== null ? (string)$command : 'UPDATE';
        $body = $this->generateUpdateBody($table, $data, $where, $parameters);
        if ($body === false) {
            return false;
        }
        if ($options) {
            if ($options & self::OPTIONS_IGNORE) {
                $command .= ' IGNORE';
            }
            if ($options & self::OPTIONS_SINGLE_ROW) {
                $body .= ' LIMIT 1';
            }
        }
        return "{$command} {$body}";
    }

    protected function generateAssignmentList($data, $parameters = null)
    {
        $parts = array();
        foreach ($data as $field => $value) {
            $f = $this->quoteSimpleName($field);
            $v = ($value instanceof DbExpression ? $value->buildExpression($this, $parameters) : $this->constant($value));
            $parts[] = $f.' = '.$v;
        }
        return \implode(', ', $parts);
    }

    protected function generateUpdateBody($table, $data, $where = null, $parameters = null)
    {
        if (!\is_array($data) || empty($data)) {
            return false;
        }

        $assignmentList = $this->generateAssignmentList($data, $parameters);
        $where = $where !== null ? $this->buildWhereString($where, $parameters) : '';
        $body = 'SET '.$assignmentList;
        if ((string)$where !== '') {
            $body .= " WHERE ".$where;
        }
        return $this->quoteNameOrExpression($table).' '.$body;
    }

    public function buildDeleteQuery($table, $where = null, $options = null, $parameters = null, $command = null)
    {
        $command = $command !== null ? (string)$command : 'DELETE';
        $body = $this->generateDeleteBody($table, $where, $parameters);
        if ($body === false) {
            return false;
        }
        if ($options) {
            if ($options & self::OPTIONS_SINGLE_ROW) {
                $body .= ' LIMIT 1';
            }
        }
        return "{$command} {$body}";
    }

    protected function generateDeleteBody($table, $where = null, $parameters = null)
    {
        $where = $where !== null ? $this->buildWhereString($where, $parameters) : '';
        if ((string)$where !== '') {
            return 'FROM '.$this->quoteNameOrExpression($table).' WHERE '.$where;
        } else {
            return 'FROM '.$this->quoteNameOrExpression($table);
        }
    }

    public function createSelect($select, $from = null, $where = null, $order = null, $limit = null, $groupBy = null, $having = null)
    {
        return $this->buildSelectQuery($select, $from, $where, $order, $limit, $groupBy, $having);
    }

    public function createSelectDistinct($select, $from = null, $where = null, $order = null, $limit = null, $groupBy = null, $having = null)
    {
        return $this->buildSelectQuery($select, $from, $where, $order, $limit, $groupBy, $having, self::OPTIONS_DISTINCT);
    }

    public function createInsert($table, $records)
    {
        return $this->buildInsertQuery($table, $records);
    }

    public function createInsertUpdate($table, $insert, $update)
    {
        return $this->buildInsertUpdateQuery($table, $insert, $update);
    }

    public function createInsertIgnore($table, $records)
    {
        return $this->buildInsertQuery($table, $records, self::OPTIONS_IGNORE);
    }

    public function createUpdate($table, $data, $where = null)
    {
        return $this->buildUpdateQuery($table, $data, $where);
    }

    public function createUpdateIgnore($table, $data, $where = null)
    {
        return $this->buildUpdateQuery($table, $data, $where, self::OPTIONS_IGNORE);
    }

    public function createDelete($table, $where = null)
    {
        return $this->buildDeleteQuery($table, $where);
    }

}
