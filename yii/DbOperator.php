<?php

namespace megabike\db\yii;

class DbOperator extends \megabike\db\DbOperator
{

    /**
     * 
     * @var \yii\db\Connection
     */
    protected $connection = null;

    /**
     * 
     * @var \yii\db\Transaction
     */
    protected $transaction = null;

    /**
     * 
     * @var type
     */
    protected $affectedRows = null;

    public function __construct($params, $connect = true)
    {
        parent::__construct($params, $connect);
        $this->enableEscapePrefix = false;
    }

    protected function getConnectionType()
    {
        return 'yii';
    }

    public function getDatabaseType()
    {
        return 'yii';
    }

    protected function isValidConnection($connection)
    {
        return $connection instanceof \yii\db\Connection;
    }

    protected function openConnection()
    {
        if (isset($this->params['db'])) {
            $db = $this->params['db'];
        } else {
            $db = 'db';
        }
        return \Yii::$app->get($db);
    }

    protected function closeConnection()
    {
        return true;
    }

    public function getLastErrorCode()
    {
        return null;
    }

    public function getLastErrorMessage()
    {
        return null;
    }

    public function executeSelectDb($dbname)
    {
        return false;
    }

    public function executeSetCharset($charset, $collation = null)
    {
        return false;
    }

    public function escapeRaw($string)
    {
        $s = (string)$this->connection->quoteValue((string)$string);
        $q = (string)$this->connection->quoteValue('');
        if (isset($q[0]) && isset($q[1]) && !isset($q[2])) {
            $e = strlen($s) - 1;
            if (isset($s[0]) && $s[0] === $q[0] && isset($s[$e]) && $s[$e] === $q[1]) {
                return substr($s, 1, -1);
            }
        }
        return $s;
    }

    public function quoteString($string)
    {
        return $this->connection->quoteValue($string);
    }

    protected function executeSimpleQuery($query)
    {
        $this->affectedRows = $this->connection->createCommand($query)->execute();
        return true;
    }

    protected function executeQuery($query)
    {
        return $this->connection->createCommand($query)->query();
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fetchAssoc($result)
    {
        return $result->read();
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fetchNumeric($result)
    {
        $result->setFetchMode(\PDO::FETCH_NUM);
        return $result->read();
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fetchBoth($result)
    {
        $result->setFetchMode(\PDO::FETCH_BOTH);
        return $result->read();
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fetchScalar($result, $columnIndex = 0)
    {
        return $result->readColumn($columnIndex);
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fieldsCount($result)
    {
        return $result->columnCount;
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fieldsMetadata($result)
    {
        return false;
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function fieldsNames($result)
    {
        return false;
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function dataSeek($result, $offset)
    {
        for ($i = 0; $i < $offset; ++$i) {
            if (!$result->read()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function numRows($result)
    {
        return $result->rowCount;
    }

    /**
     * 
     * @param \yii\db\DataReader $result
     * @return array
     */
    public function free($result)
    {
        return true;
    }

    public function affectedRows()
    {
        return $this->affectedRows;
    }

    public function insertId()
    {
        return $this->connection->getLastInsertID();
    }

    public function supportsTransactions()
    {
        return true;
    }

    public function beginTransaction()
    {
        return $this->connection->beginTransaction();
    }

    public function commit()
    {
        if ($this->transaction !== null) {
            return $this->transaction->commit();
        }
        return false;
    }

    public function rollback()
    {
        if ($this->transaction !== null) {
            return $this->transaction->rollBack();
        }
        return false;
    }

    public function supportsPreparedStatements()
    {
        return false;
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
