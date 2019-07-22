<?php
namespace Nosql\Adapter\ElasticSearch;

/**
 * 存在匹配
 * @author roy
 *
 */
class ExistsTerm extends Term
{
    public $field = "";

    /**
     *
     * @param string $field
     *            字段名
     */
    public function __construct($field)
    {
        $this->field = $field;
    }

    /**
     * 转化为数组
     */
    public function toArray()
    {
        return [ 
            "exists" => [ 
                "field" => $this->field 
            ] 
        ];
    }
}