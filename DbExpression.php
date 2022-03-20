<?php

namespace megabike\db;

class DbExpression
{
    private $expression;

    public function __construct($expression)
    {
        $this->expression = $expression;
    }

    public function getExpression()
    {
        return (string)$this->expression;
    }

    public function buildValue($operator, $parameters)
    {
        return $this;
    }

    public function buildExpression($operator, $parameters)
    {
        return $this->getExpression();
    }

    public function __toString()
    {
        return (string)$this->getExpression();
    }

}
