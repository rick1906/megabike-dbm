<?php

namespace megabike\db\mysqli;

use megabike\db\DbException;

class DbStatement extends \megabike\db\DbStatement
{
    private $metadata = null;
    private $resultVariables = null;
    private $resultValues = null;
    private $paramsTypes = null;
    private $paramsArray = null;
    private $paramsBinded = false;
    private $namedParams = array();

    public function getLastErrorCode()
    {
        return $this->statement ? \mysqli_stmt_errno($this->statement) : $this->operator->getLastErrorCode();
    }

    public function getLastErrorMessage()
    {
        return $this->statement ? \mysqli_stmt_error($this->statement) : $this->operator->getLastErrorMessage();
    }

    private function processNamedParams($query)
    {
        $this->namedParams = array();
        if (\strpos($query, ':') !== false) {
            $parts = \preg_split('/(\\\\*[\'"`])/', $query, -1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);
            $query = '';
            $index = 0;
            $unnamed = 0;
            $quoted = false;
            foreach ($parts as $part) {
                $last = \substr($part, -1);
                if ($quoted === $last || !$quoted && \strpos('\'"`', $last) !== false) {
                    $len = \strlen($part);
                    if ($len % 2 != 0) { // unescaped quote
                        $quoted = $quoted ? false : $last;
                    }
                } elseif (!$quoted) {
                    $matches = null;
                    if (\preg_match_all('/(?<!\w)(?::\w+|\?)/', $part, $matches)) {
                        $replace = array();
                        foreach ($matches[0] as $match) {
                            if ($match === '?') {
                                $this->namedParams[$unnamed++][] = $index++;
                            } else {
                                $this->namedParams[$match][] = $index++;
                                $replace[$match] = '?';
                            }
                        }
                        if ($replace) {
                            $part = \strtr($part, $replace);
                        }
                    }
                }
                $query .= $part;
            }
        }
        return $query;
    }

    protected function executePrepare($query)
    {
        return \mysqli_prepare($this->operator->getConnection(), $this->processNamedParams($query));
    }

    protected function executeStatement($statement)
    {
        if (!$this->paramsBinded && $this->paramsArray) {
            $this->applyParamsBindings();
        }
        return \mysqli_stmt_execute($statement);
    }

    protected function storeResult($success)
    {
        $this->metadata = null;
        $this->resultVariables = null;
        $this->resultValues = null;
        if ($success) {
            $this->metadata = \mysqli_stmt_result_metadata($this->statement);
            if ($this->metadata) {
                if (\mysqli_stmt_store_result($this->statement)) {
                    $this->resultVariables = array();
                    $this->resultValues = array();
                    $fieldsArray = $this->metadata->fetch_fields();
                    foreach ($fieldsArray as $field) {
                        $name = $field->name;
                        $this->resultValues[$name] = null;
                        $this->resultVariables[] = &$this->resultValues[$name];
                    }
                    if (\call_user_func_array(array($this->statement, 'bind_result'), $this->resultVariables)) {
                        return true;
                    } else {
                        throw new DbException("Failed to bind result for statement", $this);
                    }
                } else {
                    throw new DbException("Failed to store result from statement", $this);
                }
            }
        }
        return false;
    }

    protected function freeResult()
    {
        $this->metadata = null;
        $this->resultVariables = null;
        $this->resultValues = null;
        $this->paramsTypes = null;
        $this->paramsArray = null;
        $this->paramsBinded = false;
        $this->namedParams = null;
        \mysqli_stmt_free_result($this->statement);
        return true;
    }

    protected function closeStatement()
    {
        $this->metadata = null;
        $this->resultVariables = null;
        $this->resultValues = null;
        $this->paramsTypes = null;
        $this->paramsArray = null;
        $this->paramsBinded = false;
        $this->namedParams = null;
        return @\mysqli_stmt_close($this->statement);
    }

    private function buildParamTypeAndValue($value)
    {
        if (\is_int($value) || \is_bool($value)) {
            return array('i', (int)$value);
        }
        if (\is_float($value)) {
            return array('d', (float)$value);
        }
        return array('s', (string)$value);
    }

    private function applyParamsBindings()
    {
        $arguments = array();
        $arguments[0] = \implode('', $this->paramsTypes);
        for ($i = 0; $i < count($this->paramsArray); ++$i) {
            $arguments[] = &$this->paramsArray[$i];
        }
        $this->paramsBinded = \call_user_func_array(array($this->statement, 'bind_param'), $arguments);
        return $this->paramsBinded;
    }

    private function setupBinding($key, $value)
    {
        if ($this->paramsArray === null) {
            $this->paramsTypes = array();
            $this->paramsArray = array();
        }
        if (!\is_numeric($key)) {
            throw new DbException("Invalid parameter index '{$key}'");
        }
        $index = (int)$key;
        $count = \count($this->paramsArray);
        list($t, $v) = $this->buildParamTypeAndValue($value);
        if ($index > $count) {
            for ($i = 0; $i < $index - $count; ++$i) {
                $this->paramsTypes[] = 's';
                $this->paramsArray[] = null;
            }
        }
        if ($index >= $count) {
            $this->paramsTypes[] = $t;
            $this->paramsArray[] = $v;
        } elseif ($index >= 0) {
            $this->paramsTypes[$index] = $t;
            $this->paramsArray[$index] = $v;
        } else {
            throw new DbException("Invalid parameter index '{$key}'");
        }
    }

    private function setupNamedBinding($key, $value)
    {
        if (isset($this->namedParams[$key])) {
            foreach ($this->namedParams[$key] as $index) {
                $this->setupBinding($index, $value);
            }
        } else {
            throw new DbException("Parameter '{$key}' is not defined in query");
        }
    }

    protected function bindArray($array)
    {
        if ($this->namedParams) {
            $this->paramsTypes = array();
            $this->paramsArray = array();
            $this->paramsBinded = false;
            foreach ($array as $key => $value) {
                $this->setupNamedBinding($key, $value);
            }
        } else {
            $paramsTypes = array();
            $paramsArray = array();
            $index = 0;
            foreach ($array as $key => $value) {
                if (\is_int($key) && $key === $index) {
                    list($t, $v) = $this->buildParamTypeAndValue($value);
                    $paramsTypes[] = $t;
                    $paramsArray[] = $v;
                    $index++;
                } else {
                    throw new DbException("Invalid parameter index '{$key}' at position '{$index}'");
                }
            }
            $this->paramsTypes = $paramsTypes;
            $this->paramsArray = $paramsArray;
        }
        return $this->applyParamsBindings();
    }

    protected function bindOne($key, $value)
    {
        if ($this->namedParams) {
            $this->setupNamedBinding($key, $value);
        } else {
            $this->setupBinding($key, $value);
        }
        return true;
    }

    public function fetchAssoc()
    {
        $result = \mysqli_stmt_fetch($this->statement);
        $array = array();
        if ($this->resultValues !== null) {
            foreach ($this->resultValues as $key => $value) {
                $this->resultValues[$key] = null;
                $array[$key] = $value;
            }
        }
        if ($result === false || $result === null) {
            return $result;
        } else {
            return $array;
        }
    }

    public function fetchNumeric()
    {
        $result = \mysqli_stmt_fetch($this->statement);
        $array = array();
        if ($this->resultValues !== null) {
            foreach ($this->resultValues as $key => $value) {
                $this->resultValues[$key] = null;
                $array[] = $value;
            }
        }
        if ($result === false || $result === null) {
            return $result;
        } else {
            return $array;
        }
    }

    public function fetchBoth()
    {
        $result = \mysqli_stmt_fetch($this->statement);
        $array = array();
        if ($this->resultValues !== null) {
            $index = 0;
            foreach ($this->resultValues as $key => $value) {
                $this->resultValues[$key] = null;
                $array[$index++] = $value;
                $array[$key] = $value;
            }
        }
        if ($result === false || $result === null) {
            return $result;
        } else {
            return $array;
        }
    }

    public function fetchScalar($columnIndex = 0)
    {
        $row = $this->fetchBoth();
        return isset($row[$columnIndex]) ? $row[$columnIndex] : null;
    }

    public function fieldsCount()
    {
        return \mysqli_stmt_field_count($this->statement);
    }

    public function fieldsMetadata()
    {
        return $this->metadata ? $this->operator->fieldsMetadata($this->metadata) : false;
    }

    public function fieldsNames()
    {
        return $this->metadata ? $this->operator->fieldsNames($this->metadata) : false;
    }

    public function dataSeek($offset)
    {
        \mysqli_stmt_data_seek($this->statement, $offset);
        return $offset >= 0;
    }

    public function numRows()
    {
        return \mysqli_stmt_num_rows($this->statement);
    }

    public function affectedRows()
    {
        return \mysqli_stmt_affected_rows($this->statement);
    }

    public function insertId()
    {
        return $this->operator->insertId();
    }

}
