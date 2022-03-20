<?php

namespace megabike\db;

class DbReader implements \Iterator
{
    /**
     * @var DbOperator
     */
    protected $operator;
    protected $fetchType;
    protected $result;
    //
    private $index = -1;
    private $row = null;
    private $reading = null;

    public function __construct($operator, $result, $fetchType = 0)
    {
        $this->operator = $operator;
        $this->fetchType = (int)$fetchType;
        $this->result = $result;
        if (\is_scalar($this->result) || $this->result === null) {
            throw new DbException("Supplied query result is just a scalar, nothing to read from it");
        }
    }

    protected function read($fetchType)
    {
        return $this->operator->fetch($this->result, $fetchType);
    }
    
    protected function seek($offset)
    {
        return $this->operator->dataSeek($this->result, $offset);
    }

    protected function freeResult()
    {
        return $this->operator->free($this->result);
    }

    public final function current()
    {
        if ($this->reading === null) {
            $this->fetch();
        }
        return $this->row;
    }

    public final function key()
    {
        if ($this->reading === null) {
            $this->fetch();
        }
        return $this->index;
    }

    public final function valid()
    {
        if ($this->reading === null) {
            $this->fetch();
        }
        return $this->reading !== false;
    }

    public final function next()
    {
        if ($this->reading === null) {
            $this->fetch();
        }
        $this->fetch();
    }

    public final function rewind()
    {
        if ($this->reading !== null) {
            $this->dataSeek(0);
        }
    }

    public final function fetch($fetchType = null)
    {
        if ($fetchType === null) {
            $fetchType = $this->fetchType;
        }
        if ($this->reading !== false) {
            $this->index++;
            try {
                $this->row = $this->read($fetchType);
                $this->reading = $this->row !== null && $this->row !== false;
            } catch (\Exception $ex) {
                $this->row = false;
                $this->reading = false;
                throw $ex;
            }
        }
        return $this->row;
    }

    public final function fetchAll($fetchType = null)
    {
        $rows = array();
        if ($fetchType === null) {
            $fetchType = $this->fetchType;
        }
        while ($row = $this->fetch($fetchType)) {
            $rows[] = $row;
        }
        return $rows;
    }

    public final function fetchAssoc()
    {
        return $this->fetch(DbOperator::FETCH_ASSOC);
    }

    public final function fetchNumeric()
    {
        return $this->fetch(DbOperator::FETCH_NUMERIC);
    }

    public final function fetchBoth()
    {
        return $this->fetch(DbOperator::FETCH_BOTH);
    }

    public final function fetchScalar($columnIndex = 0)
    {
        $row = $this->fetch(DbOperator::FETCH_BOTH);
        return isset($row[$columnIndex]) ? $row[$columnIndex] : null;
    }

    public function fieldsCount()
    {
        return $this->operator->fieldsCount($this->result);
    }

    public function fieldsMetadata()
    {
        return $this->operator->fieldsMetadata($this->result);
    }

    public function fieldsNames()
    {
        return $this->operator->fieldsNames($this->result);
    }

    public function numRows()
    {
        return $this->operator->numRows($this->result);
    }

    public final function dataSeek($offset = 0)
    {
        if ($this->seek($offset)) {
            $this->reading = null;
            $this->row = null;
            $this->index = $offset - 1;
            return true;
        } else {
            throw new DbException("Unable to seek to offset '{$offset}'");
        }
    }

    public final function free()
    {
        $this->reading = false;
        $this->row = false;
        $this->index++;
        return $this->freeResult();
    }

}
