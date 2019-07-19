<?php
namespace Nosql\Adapter\ElasticSearch;

use Nosql\Adapter\ElasticSearch\Term;
/**
 * 模糊匹配
 * @author roy
 *
 */
class WildcardTerm extends Term
{
    public $field = "";
    public $keyword = "";

    /**
     *
     * @param string $field
     *            字段名
     * @param string $keyword
     *            匹配值,用*号代表模糊匹配
     */
    public function __construct($field, $keyword)
    {
        $this->field = $field;
        $this->keyword = $keyword;
    }

    /**
     * 转化为数组
     */
    public function toArray()
    {
        return [ 
            "wildcard" => [ 
                $this->field => $this->keyword 
            ] 
        ];
    }
}