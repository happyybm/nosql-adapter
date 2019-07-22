<?php
namespace Nosql\Adapter\ElasticSearch;

/**
 * in匹配
 * @author roy
 *
 */
class InTerm extends Term
{
    public $field = "";
    public $value = "";

    /**
     *
     * @param string $field
     *            字段名
     * @param array $value
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
            "terms" => [ 
                $this->field => $this->value 
            ] 
        ];
    }
}