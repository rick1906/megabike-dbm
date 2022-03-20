<?php

namespace megabike\db;

class DbParameter extends DbExpression
{
    private $name;

    public function __construct($name)
    {
        $this->name = ltrim((string)$name, ':');
        parent::__construct($name !== '' ? (':'.$name) : '?');
    }

    public final function getName()
    {
        return $this->name;
    }

    public function build($operator, $parameters)
    {
        
    }

}
