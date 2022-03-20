<?php

namespace megabike\db;

abstract class DbStatement
{
    /**
     * @var DbOperator
     */
    protected $operator;
    protected $query;
    protected $statement;
    protected $result;

    public function __construct($operator, $internalQuery)
    {
        $this->operator = $operator;
        $this->query = $internalQuery;
        $this->statement = $this->prepare();
        $this->result = null;
    }

    public function __destruct()
    {
        if ($this->result !== null) {
            $this->close();
        }
    }

    protected function prepare()
    {
        $exception = null;
        try {
            $statement = $this->executePrepare($this->query);
        } catch (\Exception $exception) {
            $statement = false;
        }
        if (!$statement) {
            throw new DbException("", array($this, "Statement prepare", $this->query), $exception);
        }
        return $statement;
    }

    public function execute()
    {
        $exception = null;
        try {
            $result = $this->executeStatement($this->statement);
            $this->result = $this->storeResult($result);
        } catch (\Exception $exception) {
            $result = false;
            $this->result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Statement query", $this->query), $exception);
        }
        return $this->result;
    }

    protected abstract function executePrepare($query);

    protected abstract function executeStatement($statement);

    protected abstract function storeResult($success);

    protected abstract function freeResult();

    protected abstract function closeStatement();

    protected abstract function bindArray($array);

    protected abstract function bindOne($key, $value);

    public abstract function getLastErrorMessage();

    public abstract function getLastErrorCode();

    public function free()
    {
        $r = $this->freeResult();
        $this->result = null;
        return $r;
    }

    public function close()
    {
        $r = $this->closeStatement();
        $this->result = null;
        return $r;
    }

    public function bindParameters($parameters)
    {
        if ($this->result !== null) {
            $this->free();
        }
        $exception = null;
        try {
            $result = $this->bindArray($parameters);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Bind"), $exception);
        }
        return $result;
    }

    public function bindParameter($key, $value)
    {
        if ($this->result !== null) {
            $this->free();
        }
        $exception = null;
        try {
            $result = $this->bindOne($key, $value);
        } catch (\Exception $exception) {
            $result = false;
        }
        if (!$result) {
            throw new DbException("", array($this, "Bind"), $exception);
        }
        return $result;
    }

    public function firstRow($fetchType = 0)
    {
        if ($this->result === null) {
            $this->execute();
        }
        return $this->fetch($fetchType);
    }

    public function firstScalar($columnIndex = 0)
    {
        if ($this->result === null) {
            $this->execute();
        }
        return $this->fetchScalar($columnIndex);
    }

    public function reader($fetchType = 0)
    {
        if ($this->result === null) {
            $this->execute();
        }
        return new DbStatementReader($this->operator, $this, $fetchType);
    }

    public function fetch($fetchType = 0)
    {
        if ($fetchType === DbOperator::FETCH_ASSOC) {
            return $this->fetchAssoc();
        } elseif ($fetchType === DbOperator::FETCH_NUMERIC) {
            return $this->fetchNumeric();
        } elseif ($fetchType === DbOperator::FETCH_BOTH) {
            return $this->fetchBoth();
        } else {
            throw new DbException("Unsupported fetch type");
        }
    }

    public function fetchAll($fetchType = 0)
    {
        $rows = array();
        while ($row = $this->fetch($fetchType)) {
            $rows[] = $row;
        }
        return $rows;
    }

    public abstract function fetchAssoc();

    public abstract function fetchNumeric();

    public abstract function fetchBoth();

    public abstract function fetchScalar($columnIndex = 0);

    public abstract function fieldsCount();

    public abstract function fieldsMetadata();

    public abstract function fieldsNames();

    public abstract function dataSeek($offset);

    public abstract function numRows();

    public abstract function affectedRows();

    public abstract function insertId();
}
