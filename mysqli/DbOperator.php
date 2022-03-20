<?php

namespace megabike\db\mysqli;

use megabike\db\DbException;
use megabike\db\mysql\DbCharset;

class DbOperator extends \megabike\db\DbOperator
{
    protected $mysqliDriver;
    protected $defaultReportMode;
    protected $canForcePrepare;

    public function __construct($params, $connect = true)
    {
        $this->mysqliDriver = new \mysqli_driver();
        $this->defaultReportMode = $this->mysqliDriver->report_mode;
        $this->canForcePrepare = \function_exists('mysqli_stmt_get_result') && \function_exists('mysqli_get_client_stats');
        parent::__construct($params, $connect);
    }

    protected function getConnectionType()
    {
        return 'mysqli';
    }

    public function getDatabaseType()
    {
        return 'mysql';
    }

    protected function isValidConnection($connection)
    {
        return $connection instanceof \mysqli;
    }

    protected function openConnection()
    {
        $host = $this->getHost();
        $user = $this->getUsername();
        $password = $this->getPassword();
        $database = $this->getPrimaryDatabase();
        $port = !empty($this->params['port']) ? $this->params['port'] : null;
        $exception = null;
        try {
            if ($port !== null) {
                $connection = \mysqli_connect($host, $user, $password, $database, $port);
            } else {
                $connection = \mysqli_connect($host, $user, $password, $database);
            }
        } catch (\Exception $exception) {
            $connection = false;
        }
        if (!$connection) {
            throw new DbException("", array(\mysqli_connect_error(), \mysqli_connect_errno(), "Connection"), $exception);
        }
        return $connection;
    }

    protected function closeConnection()
    {
        $this->mysqliDriver->report_mode = $this->defaultReportMode;
        return @\mysqli_close($this->connection);
    }

    protected function initConnectionFlags()
    {
        parent::initConnectionFlags();
        if ($this->enableStrictMode) {
            $this->mysqliDriver->report_mode = $this->defaultReportMode | MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT;
        } else {
            $this->mysqliDriver->report_mode = $this->defaultReportMode & ~MYSQLI_REPORT_STRICT;
        }
    }

    public function getLastErrorCode()
    {
        return \mysqli_errno($this->connection);
    }

    public function getLastErrorMessage()
    {
        return \mysqli_error($this->connection);
    }

    public function executeSelectDb($dbname)
    {
        return \mysqli_select_db($this->connection, $dbname);
    }

    public function executeSetCharset($charset, $collation = null)
    {
        $charsetManager = new DbCharset();
        $normalizedCharset = $charsetManager->normalizeCharsetName($charset);
        if ((string)$collation === '') {
            return (bool)\mysqli_set_charset($this->connection, $normalizedCharset);
        } else {
            $normalizedCollation = $charsetManager->normalizeCollation($normalizedCharset, $collation);
            return (bool)\mysqli_query($this->connection, "SET NAMES {$normalizedCharset} COLLATE {$normalizedCollation}");
        }
    }

    public function escapeRaw($string)
    {
        return \mysqli_real_escape_string($this->connection, $string);
    }

    protected function executeSimpleQuery($query)
    {
        return \mysqli_query($this->connection, $query);
    }

    protected function executeQuery($query)
    {
        if ($this->enableForcePrepare && $this->canForcePrepare && \preg_match($this->forcePrepareRegex, $query)) {
            $statement = \mysqli_prepare($this->connection, $query);
            if ($statement && \mysqli_stmt_execute($statement)) {
                $result = \mysqli_stmt_get_result($statement);
                if ($result) {
                    \mysqli_stmt_close($statement);
                    return $result;
                } else {
                    \mysqli_stmt_close($statement);
                    return true;
                }
            }
            return false;
        } else {
            return \mysqli_query($this->connection, $query);
        }
    }

    public function fetchAssoc($result)
    {
        return \mysqli_fetch_assoc($result);
    }

    public function fetchNumeric($result)
    {
        return \mysqli_fetch_row($result);
    }

    public function fetchBoth($result)
    {
        return \mysqli_fetch_array($result, MYSQLI_BOTH);
    }

    public function fetchScalar($result, $columnIndex = 0)
    {
        $row = \mysqli_fetch_array($result, MYSQLI_BOTH);
        return isset($row[$columnIndex]) ? $row[$columnIndex] : null;
    }

    public function fieldsCount($result)
    {
        return \mysqli_num_fields($result);
    }

    public function fieldsMetadata($result)
    {
        $fieldsArray = \mysqli_fetch_fields($result);
        if ($fieldsArray) {
            $fields = array();
            foreach ($fieldsArray as $field) {
                $fields[] = (array)$field;
            }
            return $fields;
        }
        return false;
    }

    public function fieldsNames($result)
    {
        $fieldsArray = \mysqli_fetch_fields($result);
        if ($fieldsArray) {
            $names = array();
            foreach ($fieldsArray as $field) {
                $names[] = $field->name;
            }
            return $names;
        }
        return false;
    }

    public function dataSeek($result, $offset)
    {
        return \mysqli_data_seek($result, $offset);
    }

    public function numRows($result)
    {
        return \mysqli_num_rows($result);
    }

    public function free($result)
    {
        return \mysqli_free_result($result);
    }

    public function affectedRows()
    {
        return \mysqli_affected_rows($this->connection);
    }

    public function insertId()
    {
        return \mysqli_insert_id($this->connection);
    }

    public function supportsTransactions()
    {
        return true;
    }

    public function beginTransaction()
    {
        if (\function_exists('mysqli_begin_transaction')) {
            return \mysqli_begin_transaction($this->connection);
        } else {
            return \mysqli_query($this->connection, "START TRANSACTION");
        }
    }

    public function commit()
    {
        return \mysqli_commit($this->connection);
    }

    public function rollback()
    {
        return \mysqli_rollback($this->connection);
    }

    public function supportsPreparedStatements()
    {
        return true;
    }

    protected function quoteEscapedString($string)
    {
        return "'{$string}'";
    }

    protected function quoteSimpleName($name)
    {
        return "`{$name}`";
    }

}
