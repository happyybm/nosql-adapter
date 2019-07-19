<?php
namespace Nosql\Adapter\ElasticSearch;

use Nosql\Adapter\ElasticSearch\Term;
/**
 * 区间查询
 * @author roy
 *
 */
class RangeTerm extends Term
{
    const OP_LT = "lt";
    const OP_GT = "gt";
    const OP_LTE = "lte";
    const OP_GTE = "gte";
    public $field = "";
    protected $ops = [ ];
    protected $values = [ ];

    /**
     *
     * @param string $field
     *            字段名
     * @param string $op1
     *            操作1
     * @param string $value1
     *            值1
     * @param string $op2
     *            操作2 可选
     * @param string $value2
     *            值2 可选
     */
    public function __construct($field, $op1, $value1, $op2 = null, $value2 = null)
    {
        $this->field = $field;
        $this->ops [] = $op1;
        $this->values [] = $value1;
        if ($op2) {
            $this->ops [] = $op2;
            $this->values [] = $value2;
        }
    }

    /**
     * 转化为数组
     */
    public function toArray()
    {
        $cond = [ ];
        foreach ( $this->ops as $k => $op ) {
            $cond [$op] = $this->values [$k];
        }
        return [ 
            "range" => [ 
                $this->field => $cond 
            ] 
        ];
    }
}