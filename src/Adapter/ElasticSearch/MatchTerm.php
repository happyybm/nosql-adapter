<?php
namespace Nosql\Adapter\ElasticSearch;

/**
 * 完全匹配
 * @author roy
 *
 */
class MatchTerm extends Term
{
    public $field = "";
    public $value = "";

    /**
     *
     * @param string $field
     *            字段名
     * @param string $value
     *            值
     */
    public function __construct($field, $value)
    {
        $this->field = $field;
        $this->value = $value;
    }

    /**
     * 转化为数组
     */
    public function toArray()
    {
        return [ 
            "term" => [ 
                $this->field => $this->value 
            ] 
        ];
    }
}