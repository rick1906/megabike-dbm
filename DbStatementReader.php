<?php

namespace megabike\db;

class DbStatementReader extends DbReader
{
    /**
     *
     * @var DbStatement
     */
    private $statement;

    public function __construct($operator, $statement, $fetchType = 0)
    {
        parent::__construct($operator, $statement, $fetchType);
        $this->statement = $statement;
    }

    protected function read($fetchType)
    {
        return $this->statement->fetch($fetchType);
    }

    protected function seek($offset)
    {
        return $this->statement->dataSeek($offset);
    }

    protected function freeResult()
    {
        return $this->statement->free();
    }

    public function fieldsCount()
    {
        return $this->statement->fieldsCount();
    }

    public function fieldsMetadata()
    {
        return $this->statement->fieldsMetadata();
    }

    public function fieldsNames()
    {
        return $this->statement->fieldsNames();
    }

    public function numRows()
    {
        return $this->statement->numRows();
    }

}
