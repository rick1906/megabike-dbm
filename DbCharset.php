<?php

namespace megabike\db;

abstract class DbCharset
{
    public function __construct()
    {
    }
    
    public abstract function normalizeCharsetName($charset);
    
    public abstract function normalizeCollation($normalizedCharset, $collation);
}
