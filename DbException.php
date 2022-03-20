<?php

namespace megabike\db;

class DbException extends \RuntimeException
{
    protected $dbErrorCode = null;
    protected $dbErrorMessage = null;

    public function __construct($message = '', $code = 0, $previous = null)
    {
        list($message, $code) = $this->preconstructor($message, $code, $previous);
        if ($previous instanceof DbException) {
            $this->dbErrorCode = $previous->getDbErrorCode();
            $this->dbErrorMessage = $previous->getDbErrorMessage();
        }
        parent::__construct($message, $code, $previous);
    }

    public function getDbErrorCode()
    {
        return $this->dbErrorCode;
    }

    public function getDbErrorMessage()
    {
        return $this->dbErrorMessage;
    }

    public static function generateMessage($dbErrorMessage, $dbErrorCode, $header = null, $query = null)
    {
        $headerPart = (string)$header !== '' ? "{$header} error" : "Error";
        $codePart = (string)$dbErrorCode !== '' ? " (code {$dbErrorCode})" : '';
        $queryPart = (string)$query !== '' ? (': '.$query) : '';
        return "{$headerPart} '{$dbErrorMessage}'{$codePart}{$queryPart}";
    }

    private function preconstructor($message, $code, $previous)
    {
        $dbErrorMessage = $message;
        $dbErrorCode = null;
        $header = null;
        $query = null;
        if (\is_array($code)) {
            $params = $code;
            $code = 0;
            $source = \array_shift($params);
            if ($source instanceof DbOperator || $source instanceof DbStatement) {
                $dbErrorMessage = $source->getLastErrorMessage();
                $dbErrorCode = $source->getLastErrorCode();
            } else {
                $dbErrorMessage = $source;
                $dbErrorCode = \array_shift($params);
            }
            $header = \array_shift($params);
            $query = \array_shift($params);
        }
        if ($code instanceof DbOperator || $code instanceof DbStatement) {
            $dbErrorMessage = $code->getLastErrorMessage();
            $dbErrorCode = $code->getLastErrorCode();
            $code = 0;
        }
        if ($previous instanceof \Exception) {
            $dbErrorMessage = $previous->getMessage();
            $dbErrorCode = null;
        }
        if ((string)$message === '') {
            $message = self::generateMessage($dbErrorMessage, $dbErrorCode, $header, $query);
        }
        $this->dbErrorMessage = $dbErrorMessage;
        $this->dbErrorCode = $dbErrorCode;
        return array($message, $code);
    }

}
