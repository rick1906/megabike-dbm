<?php

namespace megabike\db\mysql;

class DbCharset extends \megabike\db\DbCharset
{
    private static $charsetsMap = array(
        'UTF-8' => 'utf8mb4',
        'ISO 8859-2' => 'latin2',
        'EUC-JP' => 'ujis',
        'ISO 8859-8' => 'hebrew',
        'ISO 8859-7' => 'greek',
        'CP1252' => 'latin1',
        'ISO 8859-9' => 'latin5',
        'IBM850' => 'cp850',
        'IBM852' => 'cp852',
        'IBM866' => 'cp866',
        'ISO 8859-13' => 'latin7',
        'WINDOWS-1250' => 'cp1250',
        'WINDOWS-1251' => 'cp1251',
        'WINDOWS-1252' => 'latin1',
        'WINDOWS-1256' => 'cp1256',
        'WINDOWS-1257' => 'cp1257',
        'SJIS' => 'cp932',
        'EUC-JP' => 'eucjpms',
    );

    public function normalizeCharsetName($charset)
    {
        $id = strtoupper($charset);
        $result = isset(self::$charsetsMap[$id]) ? self::$charsetsMap[$id] : $charset;
        return preg_replace('/\W+/', '', strtolower($result));
    }

    public function normalizeCollation($normalizedCharset, $collation)
    {
        $collationId = strtolower($collation);
        $prefix = $normalizedCharset.'_';
        $prefixLength = strlen($prefix);
        $ending = '_ci';
        $endingLength = strlen($ending);
        if (strncasecmp($collationId, $prefix, $prefixLength)) {
            $collationId = $prefix.$collationId;
        }
        if (substr($collationId, -$endingLength) !== $ending) {
            $collationId = $collationId.$ending;
        }
        return preg_replace('/\W+/', '', $collationId);
    }

}
